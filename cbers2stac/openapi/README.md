# Creating openapi document

Checkout ```stac-api-spec``` from github.

Create a merged openapi document for each supported extension:
```bash
$ prance compile openapi.yaml openapi_compiled.yaml
```

Merge the openapi documents
```python
import hiyapyco
conf = hiyapyco.load("./core/openapi_compiled.yaml","./item-search/openapi_compiled.yaml", method=hiyapyco.METHOD_MERGE)
fp = open("merged.yaml", "w")
print(hiyapyco.dump(conf), file=fp)
```

Create the integrations with lambda.
```yaml
      summary: landing page
      tags:
      - Core
      x-amazon-apigateway-integration:
        type: aws_proxy
        httpMethod: post
        uri:
          Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${LandingEndpointLambda.Arn}/invocations
```

# On this directory

```core-item-search-integrated.yaml```: core + item-search

```core-item-search-integrated.yaml```: core + item-search, with AWS integration

```openapi_compiled_integrated.yaml```: core only, with AWS integration, to be deleted
