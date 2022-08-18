#-*- mode: makefile-gmake -*-
# Copyright (c) 2022 Peter Morgan <peter.james.morgan@gmail.com>
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
PROJECT = pgmp
PROJECT_DESCRIPTION = PostgreSQL Message Protocol
PROJECT_VERSION = ${shell git describe --tags}

DEPS = \
	envy \
	phrase \
	recon

SHELL_DEPS = \
	beaming \
	sync

TEST_DEPS = \
	proper


SHELL_OPTS = \
	-config dev.config \
	-s $(PROJECT) \
	-s sync \
	+pc unicode


PLT_APPS = \
	any \
	asn1 \
	compiler \
	crypto \
	gproc \
	inets \
	mnesia \
	phrase \
	public_key \
	runtime_tools \
	ssl \
	stdlib \
	syntax_tools \
	tools \
	xmerl

dep_beaming = git https://github.com/shortishly/beaming.git
dep_envy = git https://github.com/shortishly/envy.git
dep_phrase = git https://github.com/shortishly/phrase.git

dep_beaming_commit = main
dep_phrase_commit = main

include erlang.mk


all:: dialyze eunit
