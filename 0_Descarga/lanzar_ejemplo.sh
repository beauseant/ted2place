DAY=$(date -d "$D" '+%d')
MONTH=$(date -d "$D" '+%m') 
PREVMONTH=$(($MONTH-1))
YEAR=$(date -d "$D" '+%Y')

echo "Day: $DAY"
echo "Month: $MONTH"
echo "Previous: $PREVMONTH"
echo "Year: $YEAR"


/export/usuarios_ml4ds/sblanco/opt/anaconda3/bin/python /export/usuarios_ml4ds/sblanco/readTED/saveByXML/saveXML.py -y $YEAR -m $PREVMONTH -p /export/data_ml4ds/IntelComp/Datasets/ted/xmls/
