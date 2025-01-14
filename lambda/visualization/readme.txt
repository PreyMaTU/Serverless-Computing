How to make this work:

Setup s3 bucket in AWS

the visualization lambdas dependencies are too big to upload to lambda directly
-> create docker image and push it to aws registry:

create a user account with the right permissions (AdministratorAccess) for simplicity
create access key and save the values

follow this guide to deploy the image:
https://docs.aws.amazon.com/lambda/latest/dg/python-image.html#python-image-instructions

your commands will look like this:
docker build -t slc_vis_image .
docker tag slc_vis_image:latest <your-account-number>.dkr.ecr.eu-central-1.amazonaws.com/<your-repo-name>:latest
docker push <your-account-number>.dkr.ecr.eu-central-1.amazonaws.com/<your-repo-name>:latest

important:
the lambda needs extra memory and time
Configuration->General Configuration->Edit->
set memory to 512 mb
and timeout to 1 minute


add permissions to the lambda to read the dynamodb table and write to s3 bucket

adjust names in lambda code to match your setup

