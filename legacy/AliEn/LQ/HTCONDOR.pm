package AliEn::LQ::HTCONDOR;

#
# Initial production versions by Pavlo Svirin.
# Later versions by Maarten Litmaath & Miguel Martinez Pedreira.
#
# See:
# https://alien.web.cern.ch/content/documentation/howto/site/htcondor-based-alien-site-installation
#

@ISA = qw( AliEn::LQ );

use AliEn::LQ;
use AliEn::TMPFile;
use Data::Dumper;
use POSIX;
use strict;

sub cleanEnv
{
  my $self = shift;
  $self->debug(1, "Clean the environment");

  my $done = defined $self->{SAVED_ENV};

  $self->{SAVED_ENV} = {} unless $done;

  my $alienpath = $ENV{ALIEN_ROOT};

  my @vars = (
    "GLOBUS_LOCATION",
    "LD_LIBRARY_PATH",
    "MYPROXY_LOCATION",
    "PATH",
    "X509_CERT_DIR",
  );

  foreach (@vars) {
    $self->{SAVED_ENV}->{$_} = $ENV{$_} unless $done;

    if (/PATH$/) {
      $ENV{$_} =~ s,$alienpath[^:]*:?,,g;
    } else {
      delete $ENV{$_};
    }
  }
}

sub restoreEnv
{
  my $self = shift;
  $self->debug(1, "Restore environment");

  foreach (keys %{$self->{SAVED_ENV}}) {
    $ENV{$_} = $self->{SAVED_ENV}->{$_};
  }
}

sub _system
{
  my $self = shift;
  my $command = join(" ", @_);

  my $pid;
  my @output;
  my $msg = "timeout\n";
  my $fh;

  eval {
    local $SIG{ALRM} = sub { die $msg };

    $self->cleanEnv();
    $self->info("Doing: $command");

    alarm 60;
    $pid = open($fh, "( $command ) |") or alarm 0, die "$!\n";

    while (<$fh>) {
      push @output, $_;
    }

    alarm 0;
  };

  $self->restoreEnv();

  if ($@ eq $msg) {
    $self->{LOGGER}->error("LCG", "Timeout for: $command");
    $pid and $self->info("Timeout: killing the process $pid") and CORE::kill(9, $pid);
  } elsif ($@) {
    chomp($msg = $@);
    $self->{LOGGER}->error("LCG", "Error '$msg' for: $command");
  }

  close $fh;
  my $error = $?;

  if ($error) {
    my $sig = $error & 0x7F;
    my $val = ($error >> 8) & 0xFF;

    $error = $sig ? "signal $sig" : "status $val";

    $self->{LOGGER}->error("LCG", "Exit $error for: $command");
    $self->info("Exit $error");

    for (@output) {
      chomp;
      $self->{LOGGER}->error("LCG", "--> $_");
      $self->info("--> $_");
    }
  }

  return @output;
}

sub prepareForSubmission
{
  my $self = shift;
  $self->debug(1, "Preparing for submission...");
  my $classad = shift;
  $classad or return;

  (my $vo = $self->{CONFIG}->{LCGVO} || "ALICE") =~ tr/A-Z/a-z/;
  my $proxy_renewal = "/etc/init.d/$vo-box-proxyrenewal";

  return $classad if -e "$ENV{HOME}/no-proxy-check" || !defined $ENV{X509_USER_PROXY} ||
    !-e $proxy_renewal;

  my $thr = $self->{CONFIG}->{CE_PROXYTHRESHOLD} || 46 * 3600;

  $self->info("X509_USER_PROXY is $ENV{X509_USER_PROXY}");
  $self->info("Checking remaining proxy lifetime");

  my $command = "voms-proxy-info -acsubject -actimeleft 2>&1";

  my @lines = $self->_system($command);
  my $dn = '';
  my $timeLeft = '';

  foreach (@lines) {
    chomp;
    m|^/| and $dn = $_, next;
    m/^\d+$/ and $timeLeft = $_, next;
  }

  $dn or $self->{LOGGER}->error("LCG", "No valid proxy found.") and return;
  $self->debug(1, "DN is $dn");
  $self->info("Proxy timeleft is $timeLeft (threshold is $thr)");
  return $classad if $timeLeft > $thr;

  #
  # the proxy shall be managed by the proxy renewal service for the VO;
  # restart it as needed...
  #

  $command = "$proxy_renewal start 2>&1";
  $self->info("Checking proxy renewal service");
  @lines = $self->_system($command);

  unless ($?) {
    foreach (@lines) {
      chomp;
      $self->info($_);
    }
  }

  return $classad;
}

my $per_hold = 'periodic_hold = \
  JobStatus == 1 && ( \
    GridJobStatus =?= undefined && CurrentTime - EnteredCurrentStatus > 1800 \
  ) || \
  JobStatus <= 2 && ( \
    CurrentTime - EnteredCurrentStatus > 172800 \
  )
';

my $per_remove = 'periodic_remove = \
  CurrentTime - QDate > 259200
';

sub submit
{
  my $self = shift;
  my $classad=shift;
  my ( $command, @args ) = @_;

  my $arglist = join " ", @args;
  $self->debug(1,"*** HTCONDOR.pm submit ***");

  my $error = -2;

  $self->{COUNTER} or $self->{COUNTER} = 0;
  my $cm = "$self->{CONFIG}->{HOST}:$self->{CONFIG}->{CLUSTERMONITOR_PORT}";

  my $jobscript = $command;

  my($day, $month, $year)=(localtime)[3,4,5];
  $day   = sprintf "%02d", $day;
  $month = sprintf "%02d", $month + 1;
  $year += 1900;
  my $log_folder = "$ENV{HTCONDOR_LOG_PATH}/$year-$month-$day";
  mkdir( $log_folder ) if( $ENV{HTCONDOR_LOG_PATH} and !-d $log_folder );

  my $base = "$log_folder/jobagent_$ENV{ALIEN_JOBAGENT_ID}";
  my $log  = "log = $base.log";

  my $out = '';
  my $err = '';

  if (-e "$ENV{HOME}/enable-sandbox" || $self->{LOGGER}->getDebugLevel()) {
    $out = "output = $base.out";
    $err = "error  = $base.err";
  }

  my $osb = '+TransferOutput = ""';

# ===========

  my $submit = "cmd = $jobscript\n";

  $submit .= "$out\n$err\n$log\n" if ($ENV{HTCONDOR_LOG_PATH});

# --- via JobRouter or direct

  if ($ENV{'USE_JOB_ROUTER'}) {
    $submit .= ''
      . "universe = vanilla\n"
      . "+WantJobRouter = True\n"
      . "job_lease_duration = 7200\n"
      . "ShouldTransferFiles = YES\n";
  } else {
    $submit .= ''
      . "universe = grid\n"
      . "grid_resource = $ENV{'GRID_RESOURCE'}\n" if ($ENV{'GRID_RESOURCE'});
  }

# --- further common attributes

  $submit .= "+WantExternalCloud = True\n" if ($ENV{'USE_EXTERNAL_CLOUD'});

  $submit .= ''
    . "$osb\n"
    . "$per_hold\n"
    . "$per_remove\n"
    . "use_x509userproxy = true\n";

  my $env = ''
    . "ALIEN_CM_AS_LDAP_PROXY='$cm' "
    . "ALIEN_JOBAGENT_ID='$ENV{ALIEN_JOBAGENT_ID}'";

  $submit .= "environment = \"$env\"\n";

# --- allow preceding attributes to be overridden and others added if needed

  my $custom_file = "$ENV{HOME}/custom-classad.jdl";

  if (open(F, $custom_file)) {
    #
    # havoc may come from syntax errors or undefined attributes!
    #

    my $custom = "\n#\n# custom attributes start\n#\n\n";

    while (<F>) {
      next if m,^\s*(#.*|//.*)?$,;     # skip over comment lines
      s/\\\s*$/\\\n/;                  # remove erroneous spaces
      $custom .= $_;
    }

    close F;

    $custom .= "\n#\n# custom attributes end\n#\n\n";

    $submit .= $custom;

    $self->info("Custom attributes added from $custom_file");
  }

# --- finally

  $submit .= "queue 1\n";

# =============

  $self->{COUNTER}++;

  my $t = time >> 6;   # so that each file gets reused for 64 seconds...
  my $jdlFile = AliEn::TMPFile->new({ filename => "htc-submit.$t.jdl" })
    or return $error;

  open(F, '>', $jdlFile) or return $error;
  print F $submit;
  close F or return $error;

  my @lines = $self->_system("$self->{SUBMIT_CMD} $self->{SUBMIT_ARGS} $jdlFile");

  unless ($?) {
    foreach (@lines) {
      chomp;
      $self->info($_);
    }
  }

  #
  # By default return success even when this job submission failed,
  # because further job submissions may still work.
  #

  return ($? && -e "$ENV{HOME}/pause-on-error") ? $error : 0;
}

sub getNumberRunning
{
  #
  # the name of this subroutine is misleading:
  # it is expected to return the number of running plus idle jobs...
  #

  my $self = shift;
  my @lines = $self->_system("condor_status -schedd -af totalRunningJobs totalIdleJobs");
  return undef if $?;
  my $running = undef;

  foreach (@lines) {
    $running = $1 + $2 if /(\d+)\s+(\d+)/;
  }

  return $running;
}

sub getNumberQueued
{
  my $self = shift;
  my @lines = $self->_system("condor_status -schedd -af totalIdleJobs");
  return undef if $?;
  my $queued = undef;

  foreach (@lines) {
    $queued = $1 if /(\d+)/;
  }

  return $queued;
}

sub initialize()
{
  my $self = shift;

  $self->{PATH} = $self->{CONFIG}->{LOG_DIR};

  $self->debug(1, "In HTCONDOR.pm initialize");
  $self->{SUBMIT_CMD} = ( $self->{CONFIG}->{CE_SUBMITCMD} or "condor_submit" );
  $self->{SUBMIT_ARG} = "";

  # new style, getting extra arguments for condor_submit from LDAP or environment
  $self->{SUBMIT_ARGS} = $ENV{SUBMIT_ARGS} || "";

  $self->{USERNAME} = $ENV{LOGNAME} || $ENV{USER} || getpwuid($<);

  $self->{KILL_CMD} = ( $self->{CONFIG}->{CE_KILLCMD} or "condor_rm" );
  $self->{STATUS_CMD} = ( $self->{CONFIG}->{CE_STATUSCMD} or "condor_q" );

  $self->{GET_QUEUE_STATUS} = $self->{STATUS_CMD};

  return 1;
}

return 1;
