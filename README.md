# wiki-page-views
wiki-page-views

Step1 :
Download (wget) https://dumps.wikimedia.org/other/pageviews/ in the respective folder structure into edgenode .

Step 2: Download blacklist.txt into hdfspath
Step 3: Build the jar 
Step 4: Open the Spark Shell and execute like below
//spark2-submit --master yarn --deploy-mode client  --class GetPage_Views /linuxPath/GetPage_Views.jar 20190101 01 5
   

