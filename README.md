## Instructions

1. Run minio server.
   ```shell
   % docker run -p 9000:9000 -p 9001:9001 quay.io/minio/minio server /data --console-address ":9001"
   ```

1. Run binary

    ```shell
    % AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin go run main.go
    ```