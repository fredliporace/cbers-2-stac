# Required env vars
# DEPLOY_BUCKET

OPERATOR_EMAIL = liporace@amskepler.com
VERSION = 0-7
APP = cbers-2-stac
STACK_NAME = $(APP)-$(VERSION)
STAC_BUCKET = cbers-stac-$(VERSION)
SAM_TEMPLATE = $(APP)-template.yaml
SAM_PACKAGED = $(APP)-packaged-template.yaml
PROJECT_TAG = $(APP)-$(VERSION)

validate:
	aws cloudformation validate-template --template-body file://$(SAM_TEMPLATE)

package: update_catalog_tree/utils.py elasticsearch/utils.py stac_endpoint/utils.py elasticsearch/utils.py
	aws cloudformation package --template-file $(SAM_TEMPLATE) --s3-bucket ${DEPLOY_BUCKET} --output-template $(SAM_PACKAGED)

# @todo Use lambda layers for common modules, currently copying utils
# module to the lambda that use it. An alternative is to use symbolic links,
# currently not supported by SAM.
update_catalog_tree/utils.py: process_new_scene_queue/utils.py
	cp $? $@

elasticsearch/utils.py: process_new_scene_queue/utils.py
	cp $? $@

stac_endpoint/utils.py: process_new_scene_queue/utils.py
	cp $? $@

deploy: validate package
	aws cloudformation deploy --template-file $(SAM_PACKAGED) --stack-name $(STACK_NAME) --tags project=$(PROJECT_TAG) --capabilities CAPABILITY_IAM --parameter-overrides STACBucket=$(STAC_BUCKET) OperatorEmail=$(OPERATOR_EMAIL) StackName=$(STACK_NAME)

describe:
	aws cloudformation describe-stacks --stack-name $(STACK_NAME)
