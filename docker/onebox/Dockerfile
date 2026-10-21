FROM mcr.microsoft.com/mirror/docker/library/ubuntu:22.04@sha256:104ae83764a5119017b8e8d6218fa0832b09df65aae7d5a6de29a85d813da2fb

ENV DEBIAN_FRONTEND=noninteractive
RUN test "$(dpkg --print-architecture)" = amd64 \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates curl gnupg2 lsb-release debconf-utils locales software-properties-common \
        python3-pip build-essential iptables acl systemd sudo net-tools procps \
    && locale-gen en_US.UTF-8 \
    && rm -rf /var/lib/apt/lists/*

ENV LANG=en_US.UTF-8 LANGUAGE=en_US:en LC_ALL=en_US.UTF-8
RUN python3 -m pip install --no-cache-dir sfctl==11.1.0

RUN curl --fail --location --retry 3 \
        https://packages.microsoft.com/config/ubuntu/22.04/packages-microsoft-prod.deb \
        -o /tmp/packages-microsoft-prod.deb \
    && dpkg -i /tmp/packages-microsoft-prod.deb \
    && rm /tmp/packages-microsoft-prod.deb \
    && echo "servicefabric servicefabric/accepted-eula-ga select true" | debconf-set-selections \
    && echo "servicefabricsdkcommon servicefabricsdkcommon/accepted-eula-ga select true" | debconf-set-selections

ENV SF_VERSION=11.8.121.4
RUN mkdir -p /opt/sf-install \
    && curl --fail --location --retry 3 \
        https://download.microsoft.com/download/3/1/F/31F3FEEB-F073-4E27-A98B-8E691FF74F40/ServiceFabric.U22.11.8.121.4.deb \
        -o /opt/sf-install/servicefabric_11.8.121.4.deb \
    && echo "fbcb0b8d52e84b7e2bf9c336d9e0ece8cb9ff7fa1de53367d56c6ff8ab850c83  /opt/sf-install/servicefabric_11.8.121.4.deb" | sha256sum --check - \
    && apt-get update \
    && apt-get install -y --no-install-recommends /opt/sf-install/servicefabric_11.8.121.4.deb \
    && test "$(dpkg-query -W -f='${Version}' servicefabric)" = "$SF_VERSION" \
    && echo "c1bdc6523fea1a3711b2a4cd4d7cfa57c1133254a23c521c8054ad4d1ff0249f  /opt/microsoft/servicefabric/bin/Fabric/Fabric.Code/Fabric" | sha256sum --check - \
    && rm -rf /var/lib/apt/lists/*

ENV LD_LIBRARY_PATH=/opt/microsoft/servicefabric/bin/Fabric/Fabric.Code
LABEL io.github.service-fabric-apps.onebox="1"
COPY linux-cluster/node-entrypoint.py /usr/local/lib/sf-node/node-entrypoint.py
COPY linux-cluster/cluster-probe.py /usr/local/lib/sf-node/cluster-probe.py

WORKDIR /opt/microsoft/servicefabric/bin
ENTRYPOINT ["python3", "-u", "/usr/local/lib/sf-node/node-entrypoint.py"]
