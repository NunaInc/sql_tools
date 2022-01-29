#
# nuna_sql_tools: Copyright 2022 Nuna Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Example generating some data for in-use schema."""

from absl import app
from dataschema import schema_synth, schema_synth_generator

from pyschema.shared.Enrollment import MedicalEnrollment
from pyschema.shared.MedicalClaim import MedicalClaim
from pyschema.shared.MemberAdd import MemberAdd
from pyschema.shared.MemberCareMetrics import MemberCareMetrics
from pyschema.shared.MemberDispute import MemberDispute
from pyschema.shared.MemberStats import MemberStats
from pyschema.shared.Provider import Provider, ProviderAddress, ProviderAffiliation
from pyschema.shared.RxClaim import RxClaim

DATA_CLASSES = [
    MedicalEnrollment, MedicalClaim, MemberAdd, MemberCareMetrics,
    MemberDispute, MemberStats, Provider, ProviderAddress, ProviderAffiliation,
    RxClaim
]

# Some more meaningful generators that we apply per field name:
DEFAULT_RE = [
    ('.*_id', ('str', (12, '0123456789ABCDEF'))),
    ('(.*_|^)first_name', ('faker', 'first_name')),
    ('(.*_|^)last_name', ('faker', 'last_name')),
    ('(.*_|^)city', ('faker', 'city')),
    ('(.*_|^)state', ('faker', 'state')),
    ('(.*_|^)zip_code', ('faker', 'zipcode')),
    ('(.*_|^)sex', ('choice', (['M', 'F'],))),
    ('(.*_|^)phone', ('faker', 'phone_number')),
    ('(.*_|^)address1', ('faker', 'street_address')),
    ('(.*_|^)address2', ('val', '')),
    ('(.*_|^)ssn', ('faker', 'ssn')),
    ('(.*_|^)tin', ('faker', 'ssn')),
    ('(.*_|^)cents', ('toint', ('exp', 5000))),
    ('(.*_|^)language',
     ('choice', (['en', 'es', 'fr', 'zh', 'pt', 'vi', 'ro', 'it', 'de'],))),
]


def main(argv):
    del argv
    schema_synth_generator.generate(
        DATA_CLASSES, builder=schema_synth.Builder(re_generators=DEFAULT_RE))


if __name__ == '__main__':
    app.run(main)
