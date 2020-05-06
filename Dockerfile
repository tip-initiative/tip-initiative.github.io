FROM swaggerapi/swagger-ui
ENV SWAGGER_JSON "/a/endpoints/api.yaml"
COPY endpoints/ /a/endpooints/
