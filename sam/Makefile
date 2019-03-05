# Required env vars
# DEPLOY_BUCKET

# Primary
APP = cbers-2-stac
STACK_NAME = cbers-2-stac-0-6
STAC_BUCKET = cbers-stac-0-6
OPERATOR_EMAIL = liporace@amskepler.com

# Derived
SAM_TEMPLATE = $(APP)-template.yaml
SAM_PACKAGED = $(APP)-packaged-template.yaml
PROJECT_TAG = $(APP)

validate:
	aws cloudformation validate-template --template-body file://$(SAM_TEMPLATE)

package:
	aws cloudformation package --template-file $(SAM_TEMPLATE) --s3-bucket ${DEPLOY_BUCKET} --output-template $(SAM_PACKAGED) 

update_catalog_tree/utils.py: process_new_scene_queue/utils.py
	cp $? $@

deploy: validate package update_catalog_tree/utils.py
	aws cloudformation deploy --template-file $(SAM_PACKAGED) --stack-name $(STACK_NAME) --tags project=$(PROJECT_TAG) --capabilities CAPABILITY_IAM --parameter-overrides STACBucket=$(STAC_BUCKET) OperatorEmail=$(OPERATOR_EMAIL) StackName=$(STACK_NAME)

describe:
	aws cloudformation describe-stacks --stack-name $(STACK_NAME)
