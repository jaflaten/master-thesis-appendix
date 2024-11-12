package AliEn::Service::Optimizer::Catalogue::QuotaUsers;

use strict;

use AliEn::Service::Optimizer::Catalogue;
use AliEn::Database::IS;


use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Catalogue");

sub checkWakesUp {
  my $self=shift;
  my $silent=shift;
  $self->{SLEEP_PERIOD} = 120;

  my $method="info";
  my @data;

  $silent=0;
  $silent and $method="debug" and push @data, 1;

  (-f "$self->{CONFIG}->{TMP_DIR}/AliEn_TEST_SYSTEM") or $self->{SLEEP_PERIOD}=600;

  $self->$method(@data, "The Catalogue/Quota optimizer starts");
  $self->{CATALOGUE}->execute("calculateFileQuota", $silent, "1");
  $self->$method(@data, "The Catalogue/Quota optimizer finished");

  return;
}

1;

