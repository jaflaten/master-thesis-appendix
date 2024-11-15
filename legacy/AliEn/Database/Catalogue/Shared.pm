package AliEn::Database::Catalogue::Shared;
use strict;

use Data::Dumper;

use vars qw(@ISA $DEBUG);
#This array is going to contain all the connections of a given catalogue
my %Connections;
push @ISA, qw(AliEn::Database);

# This function is inherited by the children
#
#
sub preConnect{
  my $self=shift;
  
  if (!$self->{UNIQUE_NM}){
    $self->{UNIQUE_NM}=time;
    #make sure that the number is unique
    while ($Connections{$self->{UNIQUE_NM}}){
      $self->{UNIQUE_NM}.="-1";
    }
    $Connections{$self->{UNIQUE_NM}}={FIRST_DB=>$self};
  }
  $self->{FIRST_DB}=$Connections{$self->{UNIQUE_NM}}->{FIRST_DB};
  $self->{DB} and $self->{HOST} and $self->{DRIVER} and return 1;
#! ($self->{DB} and $self->{HOST} and $self->{DRIVER} ) or (!$self->{CONFIG}->{CATALOGUE_DATABASE}) and  return;
  $self->debug(2, "Using the default $self->{CONFIG}->{CATALOGUE_DATABASE}");
  ($self->{HOST}, $self->{DRIVER}, $self->{DB})
    =split ( m{/}, $self->{CONFIG}->{CATALOGUE_DATABASE});

  return 1;
}

sub initialize {
  my $self=shift;
  
  $self->{CURHOSTID}=$self->queryValue("SELECT hostIndex from HOSTS where address='$self->{HOST}' and driver='$self->{DRIVER}' and db='$self->{DB}'"); 
  $self->{CURHOSTID} or $self->info("Warning this host is not in the HOSTS table!!!") and return $self->SUPER::initialize(@_);

  $self->{binary2string}=$self->binary2string("guid");
  my $dbindex="$self->{CONFIG}->{ORG_NAME}_$self->{CURHOSTID}";

  $Connections{$self->{UNIQUE_NM}}->{$dbindex}=$self;
  $self->{VIRTUAL_ROLE} or $self->{VIRTUAL_ROLE}=$self->{ROLE};
  return $self->SUPER::initialize(@_);
}

##############################################################################
##############################################################################
sub setIndexTable {
  my $self=shift;
  my $table=shift;
  my $lfn=shift;
  defined $table or return;
  $table =~ /^\d*$/ and $table="D${table}L";

  $DEBUG and $self->debug(2, "Setting the indextable to $table ($lfn)");
  $self->{INDEX_TABLENAME}={name=>$table, lfn=>$lfn};
  return 1;
}
sub getIndexTable {
  my $self=shift;
  return $self->{INDEX_TABLENAME};
}

sub getSENumber{
  my $self=shift;
  my $se=shift;
  my $options=shift || {};
  $DEBUG and $self->debug(2, "Checking the senumber");
  defined $se or return 0;
  $options->{force} and  AliEn::Util::deleteCache($self);
  my $cache=AliEn::Util::returnCacheValue($self, "seNumber-$se");
  $cache and return $cache;

  $DEBUG and $self->debug(2, "Getting the numbe from the list");
  my $senumber=$self->queryValue("SELECT seNumber FROM SE where seName=?", undef,
				 {bind_values=>[$se]});
  if (defined $senumber) {
    AliEn::Util::setCacheValue($self, "seNumber-$se", $senumber);
    return $senumber;
  }
  $DEBUG and $self->debug(2, "The entry did not exist");
  $options->{existing} and return;
  $self->{SOAP} or $self->{SOAP}=new AliEn::SOAP 
    or return ;

  my $result=$self->{SOAP}->CallSOAP("Authen", "addSE", $se) or return;
  my $seNumber=$result->result;
  $DEBUG and $self->debug(1,"Got a new number $seNumber");
  AliEn::Util::setCacheValue($self, "seNumber-$se", $senumber);

  return $seNumber;
}

##############################################################################
##############################################################################
sub actionInIndex {
  my $self=shift;
  my $action=shift;

  #updating the D0 of all the databases
  my ($hosts) = $self->getAllHosts;
print "GOT the hosts\n";
  defined $hosts
    or return;
  my ( $oldHost, $oldDB, $oldDriver ) = ($self->{HOST}, $self->{DB}, $self->{DRIVER});
  my $tempHost;
  foreach $tempHost (@$hosts) {
    #my ( $ind, $ho, $d, $driv ) = split "###", $tempHost;
    $self->info( "Updating the INDEX table of  $tempHost->{db}");
    my ($db, $table)=$self->reconnectToIndex( $tempHost->{hostIndex}, "", $tempHost );
    $db or print "Error reconecting\n" and return;
    $db->do($action) or print STDERR "Warning: Error doing $action";
  }
  $self->reconnect( $oldHost, $oldDB, $oldDriver ) or return;

  $DEBUG and $self->debug(2, "Everything is done!!");

  return 1;
}

sub insertInIndex {
  my $self=shift;
  my $hostIndex=shift;
  my $table=shift;
  my $lfn=shift;
  my $options=shift;

  $table=~ s/^D(\d+)L$/$1/;
  my $indexTable="INDEXTABLE";
  my $column="lfn";
  my $value="'$lfn'";
  if ($options->{guid}){
    $table=~ s/^G(\d+)L$/$1/;
    $column="guidTime";
    $indexTable="GUIDINDEX";
    $value="string2date('$lfn')";
  }
  $indexTable=~ /GUIDINDEX/ and $column='guidTime';
  my $action="INSERT INTO $indexTable (hostIndex, tableName, $column) values('$hostIndex', '$table', $value)";
  return $self->actionInIndex($action);
}
sub deleteFromIndex {
  my $self=shift;
  my @entries=@_;

  map {$_="lfn like '$_'"} @entries;
  my $indexTable="INDEXTABLE";
  $self->info("Ready to delete the index for @_");
  if ($_[0]=~ /^guid$/){
    $self->info("Deleting from the guidindex");
    $indexTable="GUIDINDEX";
    shift;
    @entries=@_;
    @entries=map { $_="guidTime = '$_'"} @entries;
  }

  my $action="DELETE FROM $indexTable WHERE ".join(" or ", @entries);
  return $self->actionInIndex($action);
  
}
sub getAllIndexes {
  my $self=shift;
  return $self->query("SELECT * FROM INDEXTABLE");
  
}

=item executeInAllDB ($method, @args)

This subroutine calls $method in all the databases that belong to the catalogue
If any of the calls fail, it returns udnef. Otherwise, it returns 1, and a list of the return of all the statements. 

At the end, it reconnects to the initial database

=cut

sub executeInAllDB{
  my $self=shift;
  my $method=shift;


  $DEBUG and $self->debug(1, "Executing $method (@_) in all the databases");
  my $hosts=$self->getAllHosts("hostIndex");
  my ( $oldHost, $oldDB, $oldDriver) = 
    ($self->{HOST}, $self->{DB}, $self->{DRIVER});

  my $error=0;
  my @return;
  foreach my $entry (@$hosts){
    $DEBUG and $self->debug(1, "Checking in the table $entry->{hostIndex}");
    my ($db, $path2)=$self->reconnectToIndex( $entry->{hostIndex});
    if (!$db){
      $error=1;
      last;
    }

    my $info=$db->$method(@_);
    if (!$info) {
      $error=1;
      last;
    }
    push @return, $info;
  }

  $error and return;
  $DEBUG and $self->debug(1, "Executing in all databases worked!! :) ");
  return 1, @return;

}


sub destroy {
  my $self=shift;
 
  my $number=$self->{UNIQUE_NM};
  $number or return;
  $Connections{$number} or return;
  my @databases=keys %{$Connections{$number}};

  foreach my $database (@databases){
    $database=~ /^FIRST_DB$/ and next;

    $Connections{$number}->{$database} and $Connections{$number}->{$database}->SUPER::destroy();
    delete $Connections{$number}->{$database};
  }
#  delete $Connections{$number};
  keys %Connections or undef %Connections;

#  $self->SUPER::destroy();
}

sub checkSETable {
  my $self = shift;
  
  my %columns = (seName=>"varchar(60) character set latin1 collate latin1_general_ci NOT NULL", 
		 seNumber=>"int(11) NOT NULL auto_increment primary key",
		 seQoS=>"varchar(200) character set latin1 collate latin1_general_ci",
		 seioDaemons=>"varchar(255)",
		 seStoragePath=>"varchar(255)",
		 seNumFiles=>"bigint",
		 seUsedSpace=>"bigint",
		 seType=>"varchar(60)",
		 seMinSize=>"int default 0",
         seExclusiveWrite=>"varchar(300) character set latin1 collate latin1_general_ci",
         seExclusiveRead=>"varchar(300) character set latin1 collate latin1_general_ci",
         seVersion=>"varchar(300)",
         sedemotewrite=>"float default 0",
		 sedemoteread=>"float default 0",
		);

  return $self->checkTable("SE", "seNumber", \%columns, 'seNumber', ['UNIQUE INDEX (seName)'], {engine=>"engine=innodb"} ); #or return;
  #This table we want it case insensitive
#  return $self->do("alter table SE  convert to CHARacter SET latin1");
}

sub reconnectToIndex {
  my $self=shift;
  my $index=shift;
  my $tableName=shift;
  my $data=shift;
  ($index eq $self->{CURHOSTID}) and return ($self, $tableName);
  $self->debug(2,"We have to reconnect to $index, and we are $self->{CURHOSTID}");

  # we check local cache, global cache or db
  if(!$data){
    $data = AliEn::Util::returnCacheValue($self, "hosts_$index");
        
    my $keyCache = "";
    my $ok = 0;    
        
    if(!$data){  
      if($self->{CONFIG}->{CACHE_SERVICE_ADDRESS}){
    	$keyCache = "$self->{CONFIG}->{CACHE_SERVICE_ADDRESS}?ns=hosts&key=hostindex_$index";
		($ok, $data) = AliEn::Util::getURLandEvaluate($keyCache, 1);
		$ok and AliEn::Util::setCacheValue($self, "hosts_$index", $data, 600);
      }
    
      if(!$ok){
        $data= $self->getFieldsFromHosts($index,"organisation,address,db,driver");
        $data and AliEn::Util::setCacheValue($self, "hosts_$index", $data, 600); 
    	$data and $self->{CONFIG}->{CACHE_SERVICE_ADDRESS} and 
      	  AliEn::Util::getURLandEvaluate("$keyCache&timeout=600&value=".Dumper([$data]));
      }
    }
  }
    
  ## add db error message
  defined $data
    or $self->info("Can't get the info of '$index'") and return;

  $data->{organisation} or $data->{organisation}=$self->{CONFIG}->{ORG_NAME};
  my $dbindex="$self->{CONFIG}->{ORG_NAME}_$index";
  my $changeOrg=0;
  $DEBUG and $self->debug(1, "We are in org $self->{CONFIG}->{ORG_NAME} and want to contact $data->{organisation}");
  if ($tableName and ($data->{organisation} ne $self->{CONFIG}->{ORG_NAME})) {
    $self->info("We are connecting to a different organisation");
    $self->{CONFIG}=$self->{CONFIG}->Reload({organisation=>$data->{organisation}});
    $self->{CONFIG} or $self->info("Error getting the new configuration") and return;
    $tableName =~ s/\/$//;
    $self->{"MOUNT_$data->{organisation}"}=$tableName;
    
    $self->{MOUNT}.=$self->{"MOUNT_$data->{organisation}"};
    $self->info("Mount point:$self->{MOUNT}");
    $changeOrg=1;
  }


  if ( !$Connections{$self->{UNIQUE_NM}}->{$dbindex} ) {
    #    if ( !$self->{"DATABASE_$index"} ) {
    $DEBUG and $self->debug(1,"Connecting for the first time to $data->{address} $data->{db}" );
    # CHECK LOGGER!!
    my $DBOptions={
		   "DB"     => $data->{db},
		   "HOST"   => $data->{address},
		   "DRIVER" => $data->{driver},
		   "DEBUG"  => $self->{DEBUG},
		   "USER"   => $self->{USER},
		   "SILENT" => 1,
		   "TOKEN"  => $self->{TOKEN},
		   "LOGGER" => $self->{LOGGER},
		   "ROLE"   => $self->{ROLE},
		   "VIRTUAL_ROLE" =>$self->{VIRTUAL_ROLE},
		   "FORCED_AUTH_METHOD" => $self->{FORCED_AUTH_METHOD},
		   "UNIQUE_NM"=>$self->{UNIQUE_NM},
		  };
    $self->{PASSWD} and $DBOptions->{PASSWD}=$self->{PASSWD};
    defined $self->{USE_PROXY} and $DBOptions->{USE_PROXY}=$self->{USE_PROXY};

    my $class=ref $self;
    my $db=$class->new($DBOptions )
	or print STDERR "ERROR GETTING THE NEW DATABASE\n" and return;

    $Connections{$self->{UNIQUE_NM}}->{$dbindex}=$db;
    if ($changeOrg) {
      #In the new organisation, the index is different
      my ($newIndex)= $Connections{$self->{UNIQUE_NM}}->{$dbindex}->getHostIndex($data->{address}, $data->{db});
      $DEBUG and $self->debug(1, "Setting the new index to $newIndex");
      $self->{"DATABASE_$data->{organisation}_$newIndex"}=$self->{DATABASE};
      $DEBUG and $self->debug(1, "We should do selectDatabase again");
    }

  }
  return  ($Connections{$self->{UNIQUE_NM}}->{$dbindex}, $tableName);
}

sub checkUserGroup{
  my $self = shift;
  my $user = shift
    or $self->debug(2,"In checkUserGroup user is missing")
      and return;
  my $group = shift
    or $self->debug(2,"In checkUserGroup group is missing")
      and return;

  $DEBUG and $self->debug(2,"In checkUserGroup checking if user $user is member of group $group");
  $self->queryValue("SELECT count(*) from GROUPS where Username='$user' and Groupname = '$group'");
}

sub getAllHosts {
  my $self = shift;
  my $attr = shift || "*";
  my $allOrgs=  shift;
  my $query="SELECT $attr FROM HOSTS";
  $allOrgs or $query.=" WHERE organisation is null";

  $self->query($query);
}


sub getFieldsFromHosts{
	my $self = shift;
	my $host = shift
		or $self->{LOGGER}->error("Catalogue","In getFieldsFromHosts host index is missing")
		and return;
	my $attr = shift || "*";

	$DEBUG and $self->debug(2,"In getFieldFromHosts fetching value of attributes $attr for host index $host");
	$self->queryRow("SELECT $attr FROM HOSTS WHERE hostIndex = '$host'");
}

# Gives the userid of a user and group. If the group is not specified, it
# gives the primary group
sub getUserid{
  my $self=shift;
  my $user=shift;
  my $group=shift;
  my $where="primarygroup=1";
  $group and $where="groupname='$group'";
  return $self->queryValue("SELECT userid from GROUPS where Username='$user' and $where");
}


sub setUserGroup{
  my $self=shift;
  my $user=shift;
  my $group=shift;
  my $changeUser=shift;

  my $field="ROLE";
  $changeUser or $field="VIRTUAL_ROLE";

  $self->debug(1,"Setting the userid to $user ($group)");
  $self->{$field}=$user;
  $self->{MAINGROUP}=$group;
  foreach my $index (keys %{$Connections{$self->{UNIQUE_NM}}}){
    $Connections{$self->{UNIQUE_NM}}->{$index}->{$field}=$user;
    $Connections{$self->{UNIQUE_NM}}->{$index}->{MAINGROUP}=$group;
  }
  return 1;
}

sub printConnections{
  my $self=shift;
  print "DE MOMENTO TENEMOS ". join (" ",  keys( %Connections)). "\n\n";
  print "Y BASES ". join (" ", keys (%{$Connections{$self->{UNIQUE_NM}}})). "\n\n";
}


sub renumberTable {
  my $self=shift;
  my $table=shift;
  my $index=shift;
  my $options=shift || {};

  my $lock="$table";
  $options->{lock} and $lock="$options->{lock} $lock";
  my $info=$self->queryValue("select max($index)-count(1) from $table");
  $info or $info=0;
  if ($info < 100000){
    $self->debug(1, "Only $info. We don't need to renumber");
    return 1;
  }

  $self->info("Let's renumber the table $table");

  $self->lock( $lock);
  my $ok=1;
  $self->do("alter table $table modify $index int(11), drop primary key,  auto_increment=1, add new_index int(11) auto_increment primary key, add unique index (guidid)") or $ok=0;
  if ($ok){
    foreach my $t (@{$options->{update}}){
      $self->debug(1, "Updating $t");
      $self->do("update $t set $index= (select new_index from $table where $index=$t.$index)") and next;
      $self->info("Error updating the table!!");
      $ok=0;
      last;
    }
  }
  if ($ok){
    $self->info("All the renumbering  worked! :)");
    $self->do("alter table $table drop column $index, change new_index $index int(11) auto_increment");
  } else {
    $self->info("The update didn't work. Rolling back");
    $self->do("alter table $table drop new_index, modify $index int(11) auto_increment primary key");
  }

  $self->unlock($table);

  return 1;

}


1;
