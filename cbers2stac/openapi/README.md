# Creating openapi document

Checkout ```stac-api-spec``` from github.

Create a merged openapi document for each supported extension, for instance:
```bash
$ pwd
github/stac-api-spec
$ prance compile ./core/openapi.yaml ./core/openapi_compiled.yaml
$ prance compile ./item-search/openapi.yaml ./item-search/openapi_compiled.yaml
$ prance compile ./fragments/query/openapi.yaml ./fragments/query/openapi_compiled.yaml
```

Merge the openapi documents
```python
import hiyapyco
conf = hiyapyco.load("./core/openapi_compiled.yaml","./item-search/openapi_compiled.yaml","./fragments/query/openapi_compiled.yaml",method=hiyapyco.METHOD_MERGE)
fp = open("merged.yaml", "w")
print(hiyapyco.dump(conf), file=fp)
```

If you have a patch with the integration for the previous OpenAPI document apply it:
```bash
$ patch core-item-search-query.yaml core-item-search.diff -o core-item-search-query-integrated.yaml
```

Create the integrations with lambda for the new endpoints, for example:
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


Create a patch file for the new integration. This will be used for the next updates:
```bash
$ diff -u core-item-search.yaml core-item-search-integrated.yaml > core-item-search.diff
```

# On this directory

```core-item-search-integrated.yaml```: core + item-search

```core-item-search-integrated.yaml```: core + item-search, with AWS integration

```openapi_compiled_integrated.yaml```: core only, with AWS integration, to be deleted
