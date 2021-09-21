# To merge CSV

month="Oct-Dec"
month_val="2"
streak_dir="C:\Users\rithomas\Downloads\1.StreakCsv\2. Interval 15mins\\${month_val}.${month}"
cd "$streak_dir"
IFS=$'\n'
for id in `ls *csv`
do
  cat "$id"
done | grep -v "Trigger" > $month.csv