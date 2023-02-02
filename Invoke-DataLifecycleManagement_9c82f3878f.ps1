param(
	[Parameter(Mandatory=$False,Position=1)]
    [object] $WebhookData,
    [Parameter(Mandatory = $true)][object]$payLoad,
    [Parameter(Mandatory = $true)][HashTable]$bootStrap, 
    [Parameter(Mandatory = $true)][PSCustomObject]$parameters,
    [Parameter(Mandatory = $true)]
	[AllowEmptyCollection()]
	[AllowNull()]
	[System.Collections.ArrayList]$outputStream
)

try {
    $region = Get-AutomationVariable -Name 'newFoundationRegion' -ErrorAction Stop
    # if the variable was defined then assume this is running in an automation account
	. .\Import-DataLifeCycleManagementCore.ps1
    . .\Import-DatabricksCore2.ps1
} 
catch {
	# The variable wasn't defined so assume it's running locally.
 	$localPath = (Get-Item -Path $PSScriptRoot).FullName
 	. "$localPath\Import-DatabricksCore2.ps1"
	. "$localPath\Import-DataLifeCycleManagementCore.ps1"
}
$syncLock = New-Object System.Threading.AutoResetEvent($true)
$serverInstance = "$($payLoad.DataLifeCycleManagement.ServerName).database.windows.net"
if ($payLoad.DataLifeCycleManagement.whatifMode -and [bool]$payLoad.DataLifeCycleManagement.whatifMode) {
	$mode = "WHATIFMODE: "
	[switch]$whatIfMode = $true
}
else {
	$mode = ""
	[switch]$whatIfMode = $false
}
if ($payLoad.DataLifeCycleManagement.debugMode -eq "true") {
	Set-PSDebug
}
else {
	Set-PSDebug -Off
}

# Stuff to do when a thread starts
$initialization = @"
try {
    `$region = Get-AutomationVariable -Name 'newFoundationRegion' -ErrorAction Stop
	. .\Import-DataLifeCycleManagementCore.ps1
} catch {
    # The variable wasn't defined so assume it's running locally.
    `$folder = (Get-Item -Path $PSScriptRoot).FullName
	. `$folder\Import-DataLifeCycleManagementCore.ps1
}
"@
$sbInitialisation = [scriptblock]::Create($initialization)

# Run the DML engine
$sbDLM = {
	$ctx = $args[0]
	$level1dir = $args[1]
	$activeDataSets = $args[2]
	$metaData = $args[3]
	$stream = $args[4]
	$parameters = $args[5]
	$syncLock = $args[6]
	#$debugMode = $args[7]
	$whatIfMode = $args[8]

	if ($whatIfMode) {
		$mode = "WHATIFMODE: "
	}
	else {
		$mode = ""
	}
	$started = [DateTime]::UtcNow
	$errorCount = 0
	$timeOut = 30
	try {
		$stream.Add("-------------------------------------------------------------------------------------------") | Out-Null
		$stream.Add("$($mode)DLM Processing Summary for $($level1dir.Path):") | Out-Null
		$stream.Add("$($mode)Started $(Get-Date -Date $started -Format s) UTC") | Out-Null
		$pipelineRuns = [System.Collections.ArrayList]@()
		Set-DataLifeCycle -parameters $parameters -directoryItem $level1dir -ctx $ctx -activeDataSets $activeDataSets -metaData $metaData -adfPipelineRuns $pipelineRuns -outputStream $stream -syncLock $syncLock -whatifMode:$whatIfMode
		$counter = 0
		while($pipelineRuns.Count -gt 0 -and $counter -lt $timeOut) {
			$completed = @()
			foreach($adfRun in $pipelineRuns) {
				$latest = Get-AzDataFactoryV2PipelineRun -DataFactoryName $parameters.parameters.dataFactoryName.value -PipelineRunId $adfRun.runid -ResourceGroupName $parameters.parameters.dataFactoryResourceGroupName.value
				if (-not $latest.RunEnd) {
					continue
				}
				else {
					$stream.Add($latest) | Out-Null
					foreach($output in (Set-Tiering -directoryItem $adfRun.directoryItem -ctx $ctx -metaData $metaData -whatifMode:$whatifMode)) {
						$stream.Add($output) | Out-Null
					}
					$completed += $adfRun
				}
			}
			foreach($adfRun in $completed) {
				$pipelineRuns.Remove($adfRun)
			}
			if ($pipelineRuns.Count -gt 0) {
				$counter++
				Start-Sleep -Seconds 20
			}
		}
		if ($counter -ge $timeOut) {
			$stream.Add("WARNING: Set-Tiering timed out for $($level1dir.Path)") | Out-Null
		}
	}
	catch {
		$errorCount++
		$stream.Add($_.Exception.Message) | Out-Null
	}
	finally {
		$ended = [DateTime]::UtcNow
		$stream.Add("$($mode)Ended $(Get-Date -Date $ended -Format s) UTC") | Out-Null
		$elapsedTime = New-TimeSpan -Start $started -End $ended
		$totalTime = "{0:HH:mm:ss}" -f ([datetime]$elapsedTime.Ticks)
		$stream.Add("Total elapsed time $totalTime") | Out-Null
	}
}
if ($whatIfMode) {
	$outputStream.Add("WARNING: Data life cycle management was run in 'WhatIfMode'.  No DLM activities were carried out on any storage account.") | Out-Null
}
$started = [DateTime]::UtcNow
$outputStream.Add("Start time: $(Get-date -Date $started -Format s) UTC") | Out-Null
$secret = Get-AzKeyVaultSecret -VaultName $parameters.parameters.keyVaultName.value -Name sqlssasuser
$password = $secret.SecretValue
$activeDataSets = Get-ActiveDatasets -queueId $payLoad.DataLifeCycleManagement.QueueID -ServerInstance $serverInstance -databaseName $payLoad.DataLifeCycleManagement.DatabaseName -password $password
if ($payload.DataLifeCycleManagement.Delta) {
	# This is a delta DLM activity that will be executed by databricks
	$metaDataSet = Get-MetaDataForQueue -queueId $payLoad.DataLifeCycleManagement.QueueID -ServerInstance $serverInstance -databaseName $payLoad.DataLifeCycleManagement.DatabaseName -password $password -delta
}
else {
	$metaDataSet = Get-MetaDataForQueue -queueId $payLoad.DataLifeCycleManagement.QueueID -ServerInstance $serverInstance -databaseName $payLoad.DataLifeCycleManagement.DatabaseName -password $password
}
if (-not $metaDataSet) {
	throw "Unable to process DLM because the specified queue item was not found"
}
#Loop through each row of data and create a new file
#The dataset contains a column named FileName that I am using for the name of the file
foreach($row in $metaDataSet) {
	# clone the row so it can be passed to a worker thread
	$dataRow = @{}
	$row | Get-Member -MemberType Properties | ForEach-Object { $dataRow[$($_.Name)] = $row."$($_.Name)" }
	$dataRow["DatasetFileName"] = $dataRow["DatasetPath"].SubString($dataRow["DatasetPath"].LastIndexof("/") + 1)
	
	# Chop leading / from meta paths. These are illegal.
	if ($dataRow.DatasetPath[0] -eq "/") {
		$dataRow.DatasetPath = $dataRow.DatasetPath.SubString(1)
	}
	if ($dataRow.ToDatasetPath[0] -eq "/") {
		$dataRow.ToDatasetPath = $dataRow.ToDatasetPath.SubString(1)
	}
	if ($dataRow["DatasetFormat"] -eq 'delta') {
		$dataRow["FileSystem"] 	= $dataRow["ToFileSystem"]
		$dataRow["DatasetPath"] = $dataRow["ToDatasetPath"]
	}
	$outputStream.Add($dataRow) | Out-Null
	if ($dataRow["QueueStatus"] -eq 'X') {
 		$outputStream.Add("$($mode)DLM Execution Starting.... ") | Out-Null
		Set-QueueStatusInitiate -queueId $payLoad.DataLifeCycleManagement.QueueID -ServerInstance $serverInstance -databaseName $payLoad.DataLifeCycleManagement.DatabaseName -metaData $dataRow -password $password -whatifMode:$whatIfMode
		try {
			Write-Debug "Queue status inside top level try block: $($dataRow["QueueStatus"])"
			# Get the AzContext for the subscription where the storage account lives. This isn't necessarily the same of application that is hosting DLM.
			$webhookDataSample = Get-WebhookDataSample -componentName $dataRow["ResourceGroupName"]
			$projectBootStrap = Get-WebhookData -webHookData $WebhookDataSample
			$projectBootStrap.DefaultProfile = (Set-AzContext -Subscription "Core Data EcoSystem-$($projectBootStrap.SubscriptionNumber)")
			$keys = Get-AzStorageAccountKey -ResourceGroupName $dataRow["ResourceGroupName"] -StorageAccountName $dataRow["StorageAccount"] -DefaultProfile $projectBootStrap.DefaultProfile
			$ctx = New-AzStorageContext -StorageAccountName $dataRow["StorageAccount"] -StorageAccountKey $keys[0].Value
			if ($dataRow["DatasetFormat"] -eq 'delta') {
				Write-Debug "Inside QueueStatus T block - request to tier delta format"
				# If the storage account isn't in the same subscription as the dlm installation we need to switch context
				$workspace = Get-AzDatabricksWorkspace -ResourceGroupName $row.ResourceGroupName -WorkspaceName $row.WorkspaceName -DefaultProfile $projectBootStrap.DefaultProfile -ErrorAction Stop
				Set-DlmForDelta -workspace $workspace -payLoad $payLoad -outputStream $outputStream -whatIfMode:$whatIfMode -defaultProfile $projectBootStrap.DefaultProfile
				# Data has already moved so tiering must be done in the target folder
				$directoryItem = Get-AzDataLakeGen2Item -FileSystem $dataRow.ToFileSystem -Path $dataRow.ToDatasetPath -Context $ctx -ErrorAction Stop
				foreach($output in (Set-Tiering -directoryItem $directoryItem -ctx $ctx -metaData $dataRow -inPlace -whatifMode:$whatifMode)) {
					$outputStream.Add($output) | Out-Null
				}
				Set-QueueStatusComplete -queueId $payLoad.DataLifeCycleManagement.QueueID -ServerInstance $serverInstance -databaseName $payLoad.DataLifeCycleManagement.DatabaseName -metaData $dataRow -password $password -whatifMode:$whatIfMode
				continue;
			}
			if ($dataRow.ChangeToTier -eq "P") {
				# ToDo- May as well do this in the data factory
				$outputStream.Add("Purging $($dataRow.DatasetPath)") | Out-Null
				Remove-AzDataLakeGen2Item -FileSystem $dataRow.FileSystem -Path $dataRow.DatasetPath -Context $ctx -Force -WhatIf:$whatifMode -ErrorAction Stop
				Set-QueueStatusComplete -queueId $payLoad.DataLifeCycleManagement.QueueID -ServerInstance $serverInstance -databaseName $payLoad.DataLifeCycleManagement.DatabaseName -metaData $dataRow -password $password -whatifMode:$whatIfMode
				continue
			}
			$outputStream.Add("$($mode)FileSystem: $($dataRow["FileSystem"])") | Out-Null
			$outputStream.Add("$($mode)DatasetPath: $($dataRow["DatasetPath"])") | Out-Null
			$topleveldirs = Get-AzDataLakeGen2ChildItem -FileSystem $dataRow["FileSystem"] -Context $ctx -Path $dataRow["DatasetPath"] -FetchProperty -ErrorAction Stop
			$throttlelimit = 10
			
			$jobs = @()
			$jobStreams = @{}
			$i = 1
			foreach($dir in $topleveldirs) {
				$jobStream = [System.Collections.ArrayList]@()
				#Invoke-Command -ScriptBlock $sbDLM -ArgumentList  $ctx, $dir, $activeDataSets, $dataRow, $jobStream, $parameters, $syncLock, $debugMode, $whatIfMode
				$jobs += Start-ThreadJob -Name "Job $i" -ScriptBlock $sbDLM -InitializationScript $sbInitialisation -ThrottleLimit $throttlelimit -ArgumentList $ctx, $dir, $activeDataSets, $dataRow, $jobStream, $parameters, $syncLock, $debugMode, $whatIfMode
				$i++
				$jobStreams["Job $i"] = $jobStream
			}
			# Get the output from each thread.  Need to check for errors here...
			$jobOutput = $jobs | Wait-Job | Receive-Job -Keep
			foreach($stream in $jobStreams.Keys) {
				if ($jobStreams[$stream].count -gt 1) {
					$outputStream.AddRange($jobStreams[$stream]) | Out-Null
				}
				else {
					$outputStream.Add($jobStreams[$stream]) | Out-Null
				}	
			}
			# There shouldn't be anyting in this stream...but just incase
			if ($jobOutput.count -gt 1) {
				$outputStream.AddRange($jobOutput) | Out-Null
			}
			elseif ($jobOutput.count -eq 1) {
				$outputStream.Add($jobOutput) | Out-Null
			}
			Set-QueueStatusComplete -queueId $payLoad.DataLifeCycleManagement.QueueID -ServerInstance $serverInstance -databaseName $payLoad.DataLifeCycleManagement.DatabaseName -metaData $dataRow -password $password -whatifMode:$whatIfMode
		} 
		catch {
			$outputStream.Add("ERROR: $($mode)$($_.Exception.Message)") | Out-Null
			Set-QueueStatusError -queueId $payLoad.DataLifeCycleManagement.QueueID -ServerInstance $serverInstance -databaseName $payLoad.DataLifeCycleManagement.DatabaseName -metaData $dataRow -password $password -ex $_ -whatifMode:$whatIfMode
		}
	} 
	else {
		$outputStream.Add("$($mode)Execution Conditions didn't match for queueID $($dataRow["QueueID"]) : QueueStatus = $($dataRow["QueueStatus"]) & DatasetFormat = $($dataRow["DatasetFormat"].ToLower()), DLM Execution aborted ! ") | Out-Null
	}
}
$ended = [DateTime]::UtcNow
$outputStream.Add("$($mode)Data life cycle management processing completed at End time : $(Get-Date -Date $ended -Format s)") | Out-Null
$elapsedTime = New-TimeSpan -Start $started -End $ended
$totalTime = "{0:HH:mm:ss}" -f ([datetime]$elapsedTime.Ticks)
$outputStream.Add("Total elapsed time $totalTime") | Out-Null
