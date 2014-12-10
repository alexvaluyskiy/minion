package Minion::Backend::Redis;
use Mojo::Base 'Minion::Backend';

use Redis::Fast;
use Mojo::Util 'md5_sum';
use Sys::Hostname 'hostname';
use Time::HiRes qw(time usleep);
use Mojo::JSON qw(decode_json encode_json);

has 'redis';

sub new {
    shift->SUPER::new(redis => Redis::Fast->new(@_));
}

sub register_worker {
    my $self = shift;
    my $redis = $self->redis;

    my $worker = {host => hostname, id => $self->_id, pid => $$, started => time};

    $redis->multi;
    $redis->hmset("minion:workers:$worker->{id}", %$worker);
    $redis->zadd("minion:list:workers", $worker->{started}, "minion:workers:$worker->{id}");
    $redis->exec;

    return $worker->{id};
}

sub list_workers {
    my ($self, $offset, $limit) = @_;
    my $redis = $self->redis;

    my @keys = $redis->zrevrange("minion:list:workers", $offset, $offset + $limit - 1);
    my @workers = map { {$redis->hgetall($_)} } @keys;

    return \@workers;
}

sub unregister_worker {
    my ($self, $worker_id) = @_;
    my $redis = $self->redis;

    $redis->multi;
    $redis->del("minion:workers:$worker_id");
    $redis->zrem("minion:list:workers", "minion:workers:$worker_id");
    $redis->exec;
}

sub worker_info {
    my ($self, $worker_id) = @_;
    return unless $worker_id;
    my $redis = $self->redis;

    my $worker = {$redis->hgetall("minion:workers:$worker_id")};
    return undef unless $worker->{id};

    my @jobs = $redis->smembers("minion:list:jobs:$worker_id");
    $worker->{jobs} = \@jobs if @jobs;

    return $worker;
}

sub enqueue {
    my ($self, $task) = (shift, shift);
    my $args    = shift // [];
    my $options = shift // {};

    my $redis = $self->redis;

    my $job = {
        args     => encode_json($args),
        created  => time,
        delayed  => $options->{delay} ? (time + $options->{delay}) : 1,
        id       => $self->_id,
        priority => $options->{priority} // 0,
        retries  => 0,
        state    => 'inactive',
        task     => $task,
    };

    $redis->hmset("minion:jobs:$job->{id}", %$job);

    my $priority = $job->{created} - $job->{priority} * 3600;
    $redis->zadd('minion:queue', $priority, "minion:jobs:$job->{id}");
    $redis->rpush('minion:alljobs', "minion:jobs:$job->{id}");

    return $job->{id};
}

sub dequeue {
    my ($self, $worker_id, $timeout) = @_;
    usleep $timeout * 1000000 unless my $job = $self->_try($worker_id);
    return $job || $self->_try($worker_id);
}

sub job_info {
    my ($self, $job_id) = @_;
    my $redis = $self->redis;

    return undef unless $job_id && $redis->exists("minion:jobs:$job_id");

    my $job = {$redis->hgetall("minion:jobs:$job_id")};
    map { $job->{$_} and $job->{$_} = decode_json $job->{$_} } qw(args result);

    return $job;
}

sub list_jobs {
    my ($self, $offset, $limit, $options) = @_;
    my $redis = $self->redis;

    my @keys = $redis->lrange('minion:alljobs', 0, -1);

    my $i = 0;
    my $count = 0;
    my @jobs;
    foreach my $key (reverse @keys) {
        next if $i++ < $offset;

        if ($options->{state}) {
            my $state = $redis->hget($key, 'state');
            next if $state ne $options->{state};
        }

        if ($options->{task}) {
            my $task = $redis->hget($key, 'task');
            next if $task ne $options->{task};
        }

        my $job = {$redis->hgetall($key)};
        $job->{args} = decode_json($job->{args}) if $job->{args};
        $job->{result} = decode_json($job->{result}) if $job->{result};

        push @jobs, $job;
        $count++;
        last if $count == $limit;
    }

    return \@jobs;
}

sub fail_job   { shift->_update(1, @_) }
sub finish_job { shift->_update(0, @_) }

# TODO: too slow, not critical
sub repair {
    my $self = shift;
    my $redis = $self->redis;

    # Check workers on this host (all should be owned by the same user)
    my $host = hostname;
    my @workers_keys = $redis->keys("minion:workers:*");
    my @workers_dead = grep { $_->{host} eq $host && !kill 0, $_->{pid} } map { {$redis->hgetall($_)} } @workers_keys;
    foreach my $worker (@workers_dead) {
        $self->unregister_worker($worker->{id});
    }

    # Abandoned jobs
    my @jobs_keys = $redis->keys("minion:jobs:*");
    my @jobs = map { {$redis->hgetall($_)} } @jobs_keys;
    @workers_keys = $redis->keys("minion:workers:*");
    my @workers_arr = map { {$redis->hgetall($_)} } @workers_keys;
    my $workers = { map { $_->{id} => 1 } @workers_arr };
    for my $job (@jobs) {
        next if $job->{state} ne 'active' || $workers->{$job->{worker}};
        @$job{qw(result state)} = (encode_json('Worker went away'), 'failed');
        $redis->hmset("minion:jobs:$job->{id}", %$job);
    }

    # Old jobs
    @jobs_keys = $redis->keys("minion:jobs:*");
    @jobs = map { {$redis->hgetall($_)} } @jobs_keys;
    my $after = time - $self->minion->remove_after;
    foreach my $job (@jobs) {
        next unless $job->{state} eq 'finished';
        next unless $job->{finished} < $after;
        $redis->del("minion:jobs:$job->{id}");
        $redis->lrem('minion:alljobs', 0, "minion:jobs:$job->{id}");
    }
}

sub reset {
    my $self = shift;
    my $redis = $self->redis;

    my @workers_keys = $redis->keys("minion:workers:*");
    $redis->del(@workers_keys) if @workers_keys;

    my @jobs_keys = $redis->keys("minion:jobs:*");
    $redis->del(@jobs_keys) if @jobs_keys;

    $redis->del('minion:queue');
    $redis->del('minion:list:workers');
    $redis->del('minion:alljobs');

    my @worker_jobs_keys = $redis->keys("minion:list:jobs:*");
    $redis->del(@worker_jobs_keys) if @worker_jobs_keys;
}

sub retry_job {
    my ($self, $job_id) = @_;
    my $redis = $self->redis;
    my $key = "minion:jobs:$job_id";

    $redis->watch($key);
    my $job = {$redis->hgetall($key)};
    return unless $job->{id};

    if ($job->{state} eq 'failed' || $job->{state} eq 'finished') {
        my $retried = time;
        $redis->multi;
        $redis->hincrby($key, 'retries', 1);
        $redis->hmset($key, 'retried', $retried, 'state', 'inactive');
        $redis->hdel($key, 'finished', 'result', 'started', 'worker');
        $redis->srem("minion:list:jobs:$job->{worker}", $job_id);

        my $priority = $retried - $job->{priority} * 3600;
        $redis->zadd('minion:queue', $priority, $key);

        $redis->exec;
        return 1;
    }

    $redis->unwatch;
    return;
}

sub remove_job {
    my ($self, $job_id) = @_;
    my $redis = $self->redis;
    my $key = "minion:jobs:$job_id";

    $redis->watch($key);
    my $job = {$redis->hgetall($key)};
    return unless $job->{id};

    if ($job->{state} eq 'inactive' || $job->{state} eq 'failed' || $job->{state} eq 'finished') {
        $redis->multi;
        $redis->del($key);
        $redis->zrem("minion:queue", $key);
        $redis->srem("minion:list:jobs:$job->{worker}", $job_id) if $job->{worker};
        $redis->lrem('minion:alljobs', 0, "minion:jobs:$job_id");
        $redis->exec;
        return 1;
    }

    $redis->unwatch;
    return;
}

sub stats {
    my $self = shift;
    my $redis = $self->redis;

    my $stats = {
        active_workers => 0,
        inactive_workers => 0,
        active_jobs => 0,
        inactive_jobs => 0,
        failed_jobs => 0,
        finished_jobs => 0
    };

    my @workers_keys = $redis->keys("minion:workers:*");
    my @jobs_keys = $redis->keys("minion:jobs:*");

    my %active_workers;
    foreach my $job_key (@jobs_keys) {
        my $state = $redis->hget($job_key, 'state');

        if ($state eq 'active') {
            my $worker = $redis->hget($job_key, 'worker');
            $active_workers{$worker} = 1;
        }

        $stats->{active_jobs}++ if $state eq 'active';
        $stats->{inactive_jobs}++ if $state eq 'inactive';
        $stats->{failed_jobs}++ if $state eq 'failed';
        $stats->{finished_jobs}++ if $state eq 'finished';
    }

    $stats->{active_workers} = keys %active_workers;
    $stats->{inactive_workers} = scalar @workers_keys - $stats->{active_workers};

    return $stats;
}

sub _id {
    my $self = shift;
    my $redis = $self->redis;

    my $id;
    do {
        $id = md5_sum(time . rand 999);
    }
    while ($redis->exists("minion:jobs:$id") && $redis->exists("minion:workers:$id"));

    return $id;
}

sub _try {
    my ($self, $worker_id) = @_;
    my $redis = $self->redis;
    my $queue_key = 'minion:queue';

    $redis->watch($queue_key);
    my $count = $redis->zcount($queue_key, '-inf', '+inf');
    return undef unless $count;

    my $now = time;
    my $job;
    for my $i (1..$count) {
        my ($key) = $redis->zrange($queue_key, $i - 1, $i);
        $job = {$redis->hgetall($key)};
        if ($job->{delayed} > $now) {
            $job = undef;
            next;
        }
        else {
            $redis->multi;
            $redis->zrem($queue_key, $key);
            $redis->exec;
            last;
        }
    }
    $redis->unwatch;

    return undef unless $job;

    @$job{qw(started state worker)} = (time, 'active', $worker_id);
    $redis->hmset("minion:jobs:$job->{id}", %$job);
    $redis->sadd("minion:list:jobs:$worker_id", $job->{id});

    $job->{args} = decode_json($job->{args}) if $job->{args};

    return $job;
}

sub _update {
    my ($self, $fail, $job_id, $result) = @_;
    my $redis = $self->redis;
    my $key = "minion:jobs:$job_id";

    $redis->watch($key);
    my $state = $redis->hget($key, 'state');

    if ($state eq 'active') {
        $redis->multi;
        $redis->hmset($key, 'finished', time, 'result', encode_json($result), 'state', $fail ? 'failed' : 'finished');
        $redis->zrem("minion:queue", $key);
        $redis->exec;
        return 1;
    }
    $redis->unwatch;

    return;
}

1;