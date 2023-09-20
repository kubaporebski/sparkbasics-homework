# Spark Basics - Homework application 

## Google Cloud Provider configuration
You need to save your Service Account keyfile into a folder named `service_account_key`. It should be a JSON file.

## How to run tests
This is the quickest way to run and test the application. Just open `MainApplicationTest.java` file and run it as a unit test.

![](docs/run_tests.png)

By default, this test will be storing temporary files in a `/tmp/m06sparkbasics-tests` directory. You can change that by providing an `TESTING_DIRECTORY` environment variable.
## How to run locally 

_note: Instructions for Windows 10_

### Preparing local data 
* There is a zip file (split into a few files) on the learning platform ready to download. 
* Unpack it to a directory `c:/temp`, so that files with data will be in `c:/temp/m06sparkbasics`.
* There should be folders: `C:/temp/m06sparkbasics/hotels` and `C:/temp/m06sparkbasics/weather`.
* This - `C:/temp/m06sparkbasics` - folder will be our working folder, where the app will get the input data and store the results. 
  Path to it will be passed as ENV variable (`HOMEWORK_DATA_DIR`) to a docker container.
* Directory with input files should look like this:

![](./docs/spark_basics(2).png)

### Running 
* Start a command line app and go into the project directory using `cd` command.
* Build a maven package: `mvn package -DskipTests=true -DisTestSkip=true`.
  
![](./docs/spark_basics(3).png)

* If you want to run the tests, feel free to do it - you need to add `OPENCAGE_API_KEY` environment variable - however it is not necessary to do so.
* Next, prepare a docker image: `docker build -t jp/sparkbasics .`

![](./docs/spark_basics(4).png)

* Lastly, run a docker container. Following command won't use OpenCage API and use local data.
```
docker run --rm -p 4040:4040 -e HOMEWORK_DATA_DIR=/homework -v "C:/temp/m06sparkbasics:/homework" --name sb jp/sparkbasics spark-submit --executor-memory 12G --driver-memory 4G --class jporebski.data.sparkbasics.MainApplication /opt/sparkbasics-1.0.0.jar
``` 
* You can use data that are stored in Google Cloud buckets. Use a following template:
```
docker run --rm -p 4040:4040 -e HOMEWORK_DATA_DIR=gs://jakub-porebski-bucket/m06/ --name sb jp/sparkbasics spark-submit --executor-memory 12G --driver-memory 4G --class jporebski.data.sparkbasics.MainApplication /opt/sparkbasics-1.0.0.jar
```
* If you want to get preview what Spark is actually doing at the moment, go to `http://localhost:4040`. You should see something like this:

![](./docs/spark_basics(5).png)

And in console it should look like this:

![](./docs/spark_basics(6).png)

* Following command will use OpenCage API. Replace `PROVIDE_YOURS` below with your OpenCage API Key.
```
docker run --rm -p 4040:4040 -e HOMEWORK_DATA_DIR=/homework -e LATLON_CORRECTOR=OpenCageLatLonCorrector -e OPENCAGE_API_KEY=PROVIDE_YOURS -v "C:/temp/m06sparkbasics:/homework" --name sb jp/sparkbasics spark-submit --executor-memory 12G --driver-memory 4G --class jporebski.data.sparkbasics.MainApplication /opt/sparkbasics-1.0.0.jar
```
* After computing will be done, console window should look like this:
  ![](./docs/spark_basics(7).png)
* Results are in the folder `C:/temp/m06sparkbasics/joined`. Let's go there and see what it's inside:
  ![](./docs/spark_basics(8).png)
* As you can see, everything run correctly.



## How to run in the cloud
_There should be a docker image created in on of the previous steps._
* I'm using Google Cloud SDK command line tool.
* Firstly, initialize cloud environment:

![](docs/gcloud_init.png)

* Deploy infrastructure with terraform.
```
terraform init
terraform plan -out terraform.plan
terraform apply terraform.plan
```
* After running `apply`, you should see something like this in the beginning:

![](docs/terraform_apply.png)

* And something like this in the ending:

![](docs/apply_results.png)

* Tag and push docker image.
```
docker tag jp/sparkbasics europe-central2-docker.pkg.dev/jporebski-proj/jporebski-repo/sparkbasics:v2
```
``` 
docker push europe-central2-docker.pkg.dev/jporebski-proj/jporebski-repo/sparkbasics:v2
```

* Launch Spark app in cluster mode on Kubernetes Cluster:
```
spark-submit --master k8s://https://34.118.75.224:443 --deploy-mode cluster --name sparkbasics --conf spark.kubernetes.container.image=europe-central2-docker.pkg.dev/jporebski-proj/jporebski-repo/sparkbasics:v2 --conf spark.kubernetes.file.upload.path=/temp --conf spark.kubernetes.driverEnv.HOMEWORK_DATA_DIR=gs://jakub-porebski-bucket/m06/ --conf spark.kubernetes.driverEnv.LATLON_CORRECTOR=OpenCageLatLonCorrector --conf spark.kubernetes.driverEnv.OPENCAGE_API_KEY=91bceb2e672e4712818c4d327d454b1f --class jporebski.data.sparkbasics.MainApplication local:///opt/sparkbasics-1.0.0.jar 
```
* Results should look like this:

![](docs/spark_submit_result.png)

* Let's take a look at the logs in GCP/Kubernetes dashboard:

![](docs/k8s_gcp_logs.png)

We can see that there is a log entry with a text 
> sparkbasics.MainApplication: All done!

So it indicates job well done!

* How is resulting data? We can check our google bucket.
* List of files. There should be one single file in the `joined` directory (besides that `_SUCCESS` file):

![](docs/google_bucket_list.png)

And when we click on that big file, details of it are shown:

![](docs/google_bucket_result.png)


* After everything is done, clean up by running `terraform destroy`.