#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM eclipse-temurin:17-jre-noble

ENV FLUSS_HOME=/opt/fluss
ENV PATH=$FLUSS_HOME/bin:$PATH

RUN groupadd --system --gid=9999 fluss && \
    useradd --system --home-dir $FLUSS_HOME --uid=9999 --gid=fluss fluss

WORKDIR $FLUSS_HOME

COPY --chown=fluss:fluss fluss-dist/src/main/resources/bin/ /opt/fluss/bin/
COPY --chown=fluss:fluss fluss-dist/src/main/resources/conf/ /opt/fluss/conf/
COPY --chown=fluss:fluss fluss-dist/src/main/resources/server.yaml /opt/fluss/conf/server.yaml
COPY docker/fluss/docker-entrypoint.sh /docker-entrypoint.sh

RUN mkdir -p /opt/fluss/lib /opt/fluss/log /opt/fluss/plugins && \
    chmod +x /docker-entrypoint.sh /opt/fluss/bin/*.sh && \
    chown -R fluss:fluss /opt/fluss

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["help"]
