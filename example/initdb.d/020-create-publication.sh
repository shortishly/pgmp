#!/usr/bin/env bash
# Copyright (c) 2023 Peter Morgan <peter.james.morgan@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

case $(psql --version | cut -d ' ' -f3) in
    1[56]*)
        psql --command="create publication pub for tables in schema current_schema"
        ;;
    *)
        psql --command="create publication pub for all tables"
        ;;
esac
