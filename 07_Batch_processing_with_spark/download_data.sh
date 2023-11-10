set -e

YEAR=$1
TAXI_COLOR=$2

URL_PREFIX="https://github.com/DataTalksClub/nyc-tlc-data/releases"

for MONTH in {1..12};do
  MONTH_F=`printf "%02d" ${MONTH}`

  URL="${URL_PREFIX}/download/${TAXI_COLOR}/${TAXI_COLOR}_tripdata_${YEAR}-${MONTH_F}.csv.gz"

  LOCAL_PREFIX="data/raw/${TAXI_COLOR}/${YEAR}/${MONTH_F}"
  LOCAL_FILE="${TAXI_COLOR}_tripdata_${YEAR}_${MONTH_F}.csv.gz"
  LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

  echo "downloading ${URL} to ${LOCAL_PATH}"
  mkdir -p ${LOCAL_PREFIX}
  wget ${URL} -O ${LOCAL_PATH}

done

