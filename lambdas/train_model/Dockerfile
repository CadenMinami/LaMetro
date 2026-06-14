# Container-image Lambda for XGBoost training. We use a container (not a zip
# Lambda) because xgboost + numpy exceed the 250 MB unzipped layer limit.
#
# x86_64 (default platform) on purpose:
#   - CI builds it natively (no QEMU emulation), and
#   - it binary-matches the x86 SageMaker XGBoost 1.7-1 inference container
#     that Phase 7c serves the pickled booster with.
#
# Base: AWS's Lambda Python 3.12 image — ships the Lambda Runtime Interface
# Client, so the container speaks the Lambda invoke API out of the box.
FROM public.ecr.aws/lambda/python:3.12

# Install deps into the Lambda task root. ${LAMBDA_TASK_ROOT} is /var/task.
COPY requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements.txt

# App code.
COPY handler.py ${LAMBDA_TASK_ROOT}/

# CMD = "<module>.<function>". The base image's entrypoint runs this handler.
CMD [ "handler.lambda_handler" ]
