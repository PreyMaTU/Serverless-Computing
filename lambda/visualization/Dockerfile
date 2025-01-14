FROM public.ecr.aws/lambda/python:3.8

COPY lambda_function.py ${LAMBDA_TASK_ROOT}/

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt -t ${LAMBDA_TASK_ROOT}/

CMD ["lambda_function.lambda_handler"]
