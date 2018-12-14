# Distraft
#
# Version       latest
FROM library/python:3.7.1

# Adding all files.
ADD requirements.txt /home/python/
ADD distraft/ /home/python/distraft/

# Installing all requirements
RUN pip install -q --upgrade pip
RUN pip install -q -r /home/python/requirements.txt
RUN mkdir -p /var/log/distraft
RUN mkdir -p /etc/distraft/

# Expose Volume
VOLUME ['/etc/distraft/']

# Exposing port 9000/udp and 5000/tcp
EXPOSE 9000/udp
EXPOSE 5000/tcp

# Entry point
WORKDIR /home/python/distraft
ENTRYPOINT ["python", "./server.py"]
