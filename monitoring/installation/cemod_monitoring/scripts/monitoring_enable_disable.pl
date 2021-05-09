#!/usr/bin/perl

use warnings;

my ${NGDB_HOME}=`. ~/.bash_profile; echo \$NGDB_HOME`; chomp(${NGDB_HOME});
require "/opt/nsn/ngdb/ifw/utils/Logging.pl";
require "${NGDB_HOME}/ifw/lib/application/utils/application_definition.pl";
use lib '/opt/nsn/ngdb/ifw/lib/common';
use commonUtilities;
my $script_dir="/opt/nsn/ngdb/monitoring/scripts";
our @cemod_cem_app_hosts;
my @all_scripts=qw/checkAggregations.py checkBacklogs.py qsCacheCountCpLevel.py qsCacheCountTableLevel.py checkURL.py checkOpenFile.py processMonitor.py checkJobs.py  yarnQueueUsage.sh usageTableCount.sh dayTableCountAndImsi.py weekTableCountAndImsi.py monthTableCountAndImsi.py mandatoryJobScheduling.py getTnpLatency.py serviceLastRestartTime.sh getBoundary.py portalquery.sh queriesCount.sh dalquerylog.py check_topology_status.pl checkConnectors.py configSnapshot.py threadCount.py getPartitionSize.py checkDiskUsage.py alertFailedJobs.py sendSummaryReport.py/;

my @count_all_scripts=qw/usageTableCount.sh dayTableCountAndImsi.py weekTableCountAndImsi.py monthTableCountAndImsi.py/;
###MAIN
my $par=@ARGV;

if ($par != 2)
{
        log_error("No/Improper arguments provided");
        log_error("usage:-\n\t\t\tTo Enable : perl /opt/nsn/ngdb/monitoring/scripts/monitoring_enable_disable.pl enable all/count_all/count_usage/count_day/count_week/count_month/processmonitor\n\t\t\tTo Disable: perl /opt/nsn/ngdb/monitoring/scripts/monitoring_enable_disable.pl disable all/count_all/count_usage/count_day/count_week/count_month/processmonitor");
        exit -1;
}

my $host=`hostname`;
log_info("Hostname is $host");
my $operation=shift(@ARGV);
my $operation_on=shift(@ARGV);

if ( (lc ($operation)  eq "disable") )
{
        if (lc ($operation_on) eq "all" )
        {
                my $num=get_ln_num($operation_on);
                my @num=split(/:/,$num);
                log_info("Disabling for the host $host");
                `sed -i "$num[0] , $num[1] s/^/#/" /var/spool/cron/root`;
                log_info("Disabled all monitoring scripts") if ($? == 0 );
                abort_process(@all_scripts);
        }
        elsif (lc ($operation_on) eq "count_all")
        {
                my $num=get_ln_num($operation_on);
                my @num=split(/:/,$num);
                `sed -i "$num[0] , $num[1] s/^/#/" /var/spool/cron/root`;
                log_info("Disabled all count scripts") if ($? == 0 );
                abort_process(@count_all_scripts);
        }
        else
        {
                if ( lc ($operation_on) eq "count_usage")
                {
                        my $check="usageTableCount.sh";
                        my $num=get_ln_num("$check");
                        `sed -i "$num s/^/#/" /var/spool/cron/root`;
                        log_info("Disabled $check scripts") if ($? == 0 );
                        abort_process("usageTableCount.sh");
                }
                elsif (lc ($operation_on) eq "count_day")
                {
                        my $check="dayTableCountAndImsi.py";
                        my $num=get_ln_num("$check");
                        `sed -i "$num s/^/#/" /var/spool/cron/root`;
                        log_info("Disabled $check scripts") if ($? == 0 );
                        abort_process("dayTableCountAndImsi.py");
                }
                elsif (lc ($operation_on) eq "count_week")
                {
                        my $check="weekTableCountAndImsi.py";
                        my $num=get_ln_num("$check");
                        `sed -i "$num s/^/#/" /var/spool/cron/root`;
                        log_info("Disabled $check scripts") if ($? == 0 );
                        abort_process("weekTableCountAndImsi.py");
                }
                elsif (lc ($operation_on) eq "count_month")
                {
                        my $check="monthTableCountAndImsi.py";
                        my $num=get_ln_num("$check");
                        `sed -i "$num s/^/#/" /var/spool/cron/root`;
                        log_info("Disabled $check scripts") if ($? == 0 );
                        abort_process("monthTableCountAndImsi.py");
                }
                elsif (lc ($operation_on) eq "processmonitor")
                {
                        my $check="processMonitor.py";
                        my $num=get_ln_num("$check");
                        `sed -i "$num s/^/#/" /var/spool/cron/root`;
                        log_info("Disabled $check scripts") if ($? == 0 );
                        abort_process("processMonitor.py");
                }
        }
}
elsif ( (lc ($operation)  eq "enable") )
{
        if (lc ($operation_on) eq "all" )
        {
                my $num=get_ln_num("$operation_on");
                my @num=split(/:/,$num);
                `sed -i "$num[0] , $num[1] s/^#*//" /var/spool/cron/root`;
                log_info("Enabled all monitoring scripts") if ($? == 0 );
        }
        elsif( lc ($operation_on) eq "count_all")
        {
                my $num=get_ln_num("$operation_on");
                my @num=split(/:/,$num);
                `sed -i "$num[0] , $num[1] s/^#*//" /var/spool/cron/root`;
                log_info("Enabled all count scripts") if ($? == 0 );
        }
        else
        {
                if ( lc ($operation_on) eq "count_usage")
                {
                        my $check="usageTableCount.sh";
                        my $num=get_ln_num("$check");
                        `sed -i "$num s/^#*//" /var/spool/cron/root`;
                        log_info("Enabled $check scripts") if ($? == 0 );
                }
                elsif (lc ($operation_on) eq "count_day")
                {
                        my $check="dayTableCountAndImsi.py";
                        my $num=get_ln_num("$check");
                        `sed -i "$num s/^#*//" /var/spool/cron/root`;
                        log_info("Enabled $check scripts") if ($? == 0 );
                }
                elsif (lc ($operation_on) eq "count_week")
                {
                        my $check="weekTableCountAndImsi.py";
                        my $num=get_ln_num("$check");
                        `sed -i "$num s/^#*//" /var/spool/cron/root`;
                        log_info("Enabled $check scripts") if ($? == 0 );
                }
                elsif (lc ($operation_on) eq "count_month")
                {
                        my $check="monthTableCountAndImsi.py";
                        my $num=get_ln_num("$check");
                        `sed -i "$num s/^#*//" /var/spool/cron/root`;
                        log_info("Enabled $check scripts") if ($? == 0 );
                }
                elsif (lc ($operation_on) eq "processmonitor")
                {
                        my $check="processMonitor.py";
                        my $num=get_ln_num("$check");
                        `sed -i "$num s/^#*//" /var/spool/cron/root`;
                        log_info("Enabled $check scripts") if ($? == 0 );
                }
        }
}

###SUBROUTINES
sub get_ln_num
{

        if ( lc($operation_on) eq "all" )
        {
                my $start=`grep -ni "Monitoring Scripts Cron entries" /var/spool/cron/root| awk -F ':' 'NR==1{print \$1}'`;
                my $line=1;
                my $end=`grep -ni "Monitoring Scripts Cron entries" /var/spool/cron/root| awk -F ':' 'NR==2{print \$1}'`;
                chomp($start);
                chomp($end);
                $start = $start + $line;
                $end = $end - $line;
                my $retur=$start.":".$end;
                return $retur;
        }
        elsif( lc($operation_on) eq "count_all" )
        {
                my $start=`grep -ni "usageTableCount.sh" /var/spool/cron/root`;
                my $end=`grep -ni "monthTableCountAndImsi.py" /var/spool/cron/root`;
                my $retur=$start.":".$end;
                return $retur;
        }
        else
        {
                my $to_check=shift(@_);
                my $ln_num=`grep -ni "$to_check" /var/spool/cron/root`;
                my @ln_num=split(/:/,$ln_num);
                return $ln_num[0];
        }
}

sub abort_process
{

        if (($operation_on eq "all" ) || ($operation_on eq "count_all"))
        {
                my @scr=@_;
                foreach my $script (@scr)
                {
                        my @arr=` ps -ef | grep -i "$script_dir/$script" | grep -v grep | awk '{print \$2}'`;

                        foreach (@arr)
                        {
                                chomp($_);
                                system("kill -9 $_ 2>&1");
                        }
                }
        }
        else
        {
                my $specific_scr=shift(@_);
                my @arr=` ps -ef | grep -i "$script_dir/$specific_scr" | grep -v grep | awk '{print \$2}'`;

                        foreach (@arr)
                        {
                                chomp($_);
                                system("kill -9 $_ 2>&1");
                        }
        }

}
