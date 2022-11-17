FROM jupyter/pyspark-notebook:2021-10-18

# Create directory for project
USER root
RUN mkdir -p /workspace
WORKDIR /workspace

# Add all files to Docker working directory
ADD requirements.txt /workspace/requirements.txt
RUN pip install -r /workspace/requirements.txt

# Add remaining files to working dir and run setup.py to install
ADD . /workspace

# Create a wheel, locate the most recent build, then install it
RUN python setup.py -q bdist_wheel

# Get most recent wheel file & install it
RUN fn=$(ls -t ./dist/*.whl | head -n1)
RUN pip install "./$fn"