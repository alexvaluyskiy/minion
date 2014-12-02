package Minion::Backend::Redis;
use Mojo::Base 'Minion::Backend';

use Redis::Fast;
use Mojo::Util 'md5_sum';
use Sys::Hostname 'hostname';
use Time::HiRes qw(time usleep);
use Mojo::JSON qw(decode_json encode_json);

use common::sense;
use Data::Dumper;

has 'redis';

sub new {
    shift->SUPER::new(redis => Redis::Fast->new(@_));
}

sub register_worker {
    my $self = shift;
    my $redis = $self->redis;

    my $worker = {host => hostname, id => $self->_id, pid => $$, started => time};

    $redis->multi;
    $redis->hmset("minion_workers:$worker->{id}", %$worker);
    $redis->zadd("minion_workers_created", $worker->{started}, "minion_workers:$worker->{id}");
    $redis->exec;

    return $worker->{id};
}

sub list_workers {
    my ($self, $offset, $limit) = @_;
    my $redis = $self->redis;

    my @keys = $redis->zrevrange("minion_workers_created", $offset, $offset + $limit - 1);
    my @workers = map { {$redis->hgetall($_)} } @keys;

    return \@workers;
}

sub unregister_worker {
    my ($self, $worker_id) = @_;
    my $redis = $self->redis;

    $redis->multi;
    $redis->del("minion_workers:$worker_id");
    $redis->zrem("minion_workers_created", "minion_workers:$worker_id");
    $redis->exec;
}

sub worker_info {
    my ($self, $worker_id) = @_;
    my $redis = $self->redis;

    return undef unless $worker_id && $redis->exists("minion_workers:$worker_id");

    my $worker = {$redis->hgetall("minion_workers:$worker_id")};

    # TODO: too slow
    my @jobs_keys = $redis->keys("minion_jobs:*");
    my @jobs = map { $_->{id} } grep { $_->{worker} eq $worker_id } map { {$redis->hgetall($_)} } @jobs_keys;
    $worker->{jobs} = \@jobs;

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

    $redis->hmset("minion_jobs:$job->{id}", %$job);
    #$redis->sadd("minion_jobs:inactive", "minion_jobs:$job->{id}");

    return $job->{id};
}

sub dequeue {
    my ($self, $id, $timeout) = @_;
    usleep $timeout * 1000000 unless my $job = $self->_try($id);
    return $job || $self->_try($id);
}

sub job_info {
    my ($self, $job_id) = @_;
    my $redis = $self->redis;

    return undef unless $job_id && $redis->exists("minion_jobs:$job_id");

    my $job = {$redis->hgetall("minion_jobs:$job_id")};
    map { $job->{$_} and $job->{$_} = decode_json $job->{$_} } qw(args result);

    return $job;
}

sub list_jobs {
    my ($self, $offset, $limit, $options) = @_;
    my $redis = $self->redis;

    my @keys = $redis->keys("minion_jobs:*");

    # TODO: too slow
    my @jobs = sort { $b->{created} <=> $a->{created} } map { {$redis->hgetall($_)} } @keys;
    @jobs = grep { $_->{state} eq $options->{state} } @jobs if $options->{state};
    @jobs = grep { $_->{task} eq $options->{task} } @jobs if $options->{task};
    @jobs = splice(@jobs, $offset, $limit);

    foreach my $job (@jobs) {
        $job->{args} = decode_json($job->{args}) if $job->{args};
        $job->{result} = decode_json($job->{result}) if $job->{result};
    }

    return \@jobs;
}

sub fail_job   { shift->_update(1, @_) }
sub finish_job { shift->_update(0, @_) }

sub repair {
    my $self = shift;
    my $redis = $self->redis;

    # Check workers on this host (all should be owned by the same user)
    my $host = hostname;
    my @workers_keys = $redis->keys("minion_workers:*");
    my @workers_dead = grep { $_->{host} eq $host && !kill 0, $_->{pid} } map { {$redis->hgetall($_)} } @workers_keys;
    foreach my $worker (@workers_dead) {
        $self->unregister_worker($worker->{id});
    }

    # Abandoned jobs
    my @jobs_keys = $redis->keys("minion_jobs:*");
    my @jobs = map { {$redis->hgetall($_)} } @jobs_keys;
    my @workers_keys = $redis->keys("minion_workers:*");
    my @workers_arr = map { {$redis->hgetall($_)} } @workers_keys;
    my $workers = { map { $_->{id} => 1 } @workers_arr };
    for my $job (@jobs) {
        next if $job->{state} ne 'active' || $workers->{$job->{worker}};
        @$job{qw(result state)} = (encode_json('Worker went away'), 'failed');
        $redis->hmset("minion_jobs:$job->{id}", %$job);
    }

    # Old jobs
    @jobs_keys = $redis->keys("minion_jobs:*");
    @jobs = map { {$redis->hgetall($_)} } @jobs_keys;
    my $after = time - $self->minion->remove_after;
    foreach my $job (@jobs) {
        next unless $job->{state} eq 'finished';
        next unless $job->{finished} < $after;
        $redis->del("minion_jobs:$job->{id}");
    }
}

sub reset {
    my $self = shift;
    my $redis = $self->redis;

    my @workers_keys = $redis->keys("minion_workers:*");
    foreach (@workers_keys) {
        $redis->del($_);
    }

    my @jobs_keys = $redis->keys("minion_jobs:*");
    foreach (@jobs_keys) {
        $redis->del($_);
    }

    $redis->del("minion_hash");
    $redis->del("minion_jobs:inactive");
    $redis->del('minion_workers_created');
}

sub retry_job {
    my ($self, $job_id) = @_;
    my $redis = $self->redis;
    my $key = "minion_jobs:$job_id";

    $redis->watch($key);
    my $state = $redis->hget($key, 'state');

    if ($state eq 'failed' || $state eq 'finished') {
        $redis->multi;
        $redis->hincrby($key, 'retries', 1);
        $redis->hmset($key, 'retried', time, 'state', 'inactive');
        $redis->hdel($key, 'finished', 'result', 'started', 'worker');
        $redis->exec;
        return 1;
    }

    $redis->unwatch;
    return;
}

sub remove_job {
    my ($self, $job_id) = @_;
    my $redis = $self->redis;
    my $key = "minion_jobs:$job_id";

    my $state = $redis->hget($key, 'state');
    if ($state eq 'inactive' || $state eq 'failed' || $state eq 'finished') {
        $redis->del($key);
        return 1;
    }

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

    my @workers_keys = $redis->keys("minion_workers:*");
    my @jobs_keys = $redis->keys("minion_jobs:*");

    my %active_workers;
    foreach (@jobs_keys) {
        my $job = {$redis->hgetall($_)};

        $active_workers{$job->{worker}} = 1 if $job->{state} eq 'active';

        $stats->{active_jobs}++ if $job->{state} eq 'active';
        $stats->{inactive_jobs}++ if $job->{state} eq 'inactive';
        $stats->{failed_jobs}++ if $job->{state} eq 'failed';
        $stats->{finished_jobs}++ if $job->{state} eq 'finished';
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
    while ($redis->sismember('minion_hash', $id));

    $redis->sadd('minion_hash', $id);

    return $id;
}

sub _try {
    my ($self, $id) = @_;
    my $redis = $self->redis;

    #my @jobs_keys = $redis->smembers("minion_jobs:inactive");
    #my @ready = map { {$redis->hgetall($_)} } @jobs_keys;
    #$redis->srem("minion_jobs:inactive", $job->{id});

    # TODO: extremly slow
    my @jobs_keys = $redis->keys("minion_jobs:*");
    my @ready = grep { $_->{state} eq 'inactive' } map { {$redis->hgetall($_)} } @jobs_keys;

    my $now = time;
    @ready = grep { $_->{delayed} < $now } @ready;
    @ready = sort { $a->{created} <=> $b->{created} } @ready;
    @ready = sort { $b->{priority} <=> $a->{priority} } @ready;

    my $job = shift @ready;
    return undef unless $job;

    @$job{qw(started state worker)} = (time, 'active', $id);
    $redis->hmset("minion_jobs:$job->{id}", %$job);

    $job->{args} = decode_json($job->{args}) if $job->{args};

    return $job;
}

sub _update {
    my ($self, $fail, $job_id, $result) = @_;
    my $redis = $self->redis;
    my $key = "minion_jobs:$job_id";

    $redis->watch($key);
    my $state = $redis->hget($key, 'state');

    if ($state eq 'active') {
        $redis->hmset($key, 'finished', time, 'result', encode_json($result), 'state', $fail ? 'failed' : 'finished');
        $redis->unwatch;
        return 1;
    }

    return;
}

1;
