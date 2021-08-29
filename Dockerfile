FROM centos:centos7
RUN yum install -y python3 python3-pip
RUN pip3 install --no-cache-dir kubernetes pyyaml
RUN yum clean all
COPY mongoScheduler.py /mongoScheduler.py
COPY helpers.py /helpers.py
RUN /bin/mkdir /data
WORKDIR /
ENTRYPOINT   ["python3", "mongoScheduler.py"]