
    Disable-AzContextAutosave -Scope Process
    Connect-AzAccount -Identity
    $AzureContext = Set-AzContext -SubscriptionId "16cbe606-c8b9-4d7b-aa44-6088630accda"


$azResourceGroup = "DefaultResourceGroup-SUK"
$azVMName= "MainRepro"


     $vm = Get-AzVM `
   -ResourceGroupName $azResourceGroup `
   -Name $azVMName




    $vm.StorageProfile.OsDisk.ManagedDisk.id
    $vm.Location
	$vm.StorageProfile.OsDisk.Name
	$vm.StorageProfile.OsDisk.ManagedDisk.StorageAccountType