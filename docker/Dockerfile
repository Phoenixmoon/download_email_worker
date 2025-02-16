FROM public.ecr.aws/lambda/python:3.11

WORKDIR ${LAMBDA_TASK_ROOT}

# Install dependencies
# RUN yum install -y mesa-libGL wget

# install cmake
# RUN yum install -y gcc-c++ make

# Install requirements
COPY docker/requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install -r requirements.txt

# Copy stuff
COPY *.py ${LAMBDA_TASK_ROOT}

# Set
CMD ["app.lambda_handler"]