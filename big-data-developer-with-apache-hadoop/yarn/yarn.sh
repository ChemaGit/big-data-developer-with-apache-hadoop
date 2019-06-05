#List running applications
	$yarn application -list
#Kill a running application
	$yarn application -kill <app-id>
#View the logs of the specified application
	$yarn logs -applicationId <app-id>
#View the full list of command options
	$yarn -help
	
#EXAMPLE
	
#Run the app wordcount.py on the YARN cluster
	$spark-submit --master yarn-client wordcount.py /loudacre/kb/*
#View the list of currently running applications
	$yarn application -list
#Maybe your app is finished. Use the appStates ALL option to include all app in the list.
	$yarn application -list -appStates ALL
#Take note of your app-id and use it in the following command
	$yarn application -status application_1511445063138_0002
#Access logs of your application
	$yarn logs -applicationId  application_1511445063138_0002
	
