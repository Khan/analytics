#!/bin/bash -e

# Fetch UserAssessment data from S3 as input to generate the models
# for assessments on the website.
#
# USAGE:
#   dt=2013-03-11 ./train_mirt_models.sh
#
# The below items beginning with ":" are configurable options that can
# be set in the environment on the command line, e.g.,
#
#   tmpdir=/tmp/mirt skip_fetch=1 ./train_mirt_models.sh
#
# which would always work in /tmp/mirt instead of a temporary
# directory, and would not fetch the S3 data (perhaps you've already
# fetched it and are editing the training scripts.

# Set -vx late to skip header comments.
set -vx

: ${tmpdir:=$(mktemp -u /tmp/mirt.XXX)}
: ${dt:=$(date -v-2d "+%Y-%m-%d")}  # default to data from 2 days ago
: ${website_dir:=~/stable}
: ${analytics_dir:=~/analytics}
: ${skip_fetch:=}  # non-empty to skip S3 download. Useful for data re-use
: ${json_outfile:="${tmpdir}/mirt_model.json"}

data_dir="${tmpdir}/UserAssessmentP/dt=${dt}"
assessment_responses="${tmpdir}/user_assessment.responses"
mirt_train_dir="${tmpdir}/mirt_train_EM"

# 0. (Optional) Fetch UserAssessment data.
mkdir -p "${data_dir}"
[[ -z "${skip_fetch}" ]] && (cd "${data_dir}" && s3cmd get --recursive "s3://ka-mapreduce/entity_store/UserAssessmentP/dt=${dt}/")

# 1. Run UserAssessment data through get_user_assessment_data.py to
# create the training data set.
zcat "${data_dir}"/*.gz | python get_user_assessment_data.py >"${assessment_responses}"

# 2. Run the output data file from (1) through mirt_train_EM.py.
mkdir -p "${mirt_train_dir}"
PYTHONPATH="${website_dir}:${analytics_dir}/src" \
  python mirt_train_EM.py -a 2 -n 75 -f "${assessment_responses}" -w 0 -o "${mirt_train_dir}/" &> "${tmpdir}/mirt_train_EM.log"

# 3. Run the last .npz file from (2) through mirt_npz_to_json.py.
# We assume mirt_train_EM.py generates files suffixed "_epoch=\d+"
latest_mirt_npz=$(cd "${mirt_train_dir}" && ls *.npz | sort -t= -k2n | tail -1)
python mirt_npz_to_json.py "${mirt_train_dir}"/"${latest_mirt_npz}" > "${json_outfile}"

# 4. Upload the json-ified model using mirt_upload_to_gae.py.
set +vx
echo
echo "Generated JSON MIRT model ${json_outfile} for dt=${dt} in ${tmpdir}."
echo "Next step is to verify your model then upload to production, i.e.,"
echo "  ${EDITOR:=vi} \"${json_outfile}\""
echo "  $(dirname $0)/mirt_upload_to_gae.py --update \"${json_outfile}\""
# To upload to a local dev_appserver, use curl to upload the json file:
#		curl -H "Content-Type: application/json" --data @fractions.json http://localhost:8080/api/v1/dev/assessment/params?auth=off
#		(where fractions.json is replaced with the appropriate json file name)
