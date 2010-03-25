properties {
  $base_dir  = resolve-path .
  $lib_dir = "$base_dir\SharedLibs"
  $build_dir = "$base_dir\build"
  $buildartifacts_dir = "$build_dir\"
  $sln_file = "$base_dir\RavenDB.sln"
  $version = "1.0.0.0"
  $tools_dir = "$base_dir\Tools"
  $release_dir = "$base_dir\Release"
}

include .\psake_ext.ps1

task default -depends Release

task Clean {
  remove-item -force -recurse $buildartifacts_dir -ErrorAction SilentlyContinue
  remove-item -force -recurse $release_dir -ErrorAction SilentlyContinue
}

task Init -depends Clean {
	Generate-Assembly-Info `
		-file "$base_dir\Raven.Database\Properties\AssemblyInfo.cs" `
		-title "Raven Database $version" `
		-description "A linq enabled document database for .NET" `
		-company "Hibernating Rhinos" `
		-product "Raven Database $version" `
		-version $version `
		-copyright "Hibernating Rhinos & Ayende Rahien 2004 - 2010" `
		-clsCompliant "false"

	Generate-Assembly-Info `
		-file "$base_dir\Raven.Client\Properties\AssemblyInfo.cs" `
		-title "Raven Database Client $version" `
		-description "A linq enabled document database for .NET" `
		-company "Hibernating Rhinos" `
		-product "Raven Database $version" `
		-version $version `
		-copyright "Hibernating Rhinos & Ayende Rahien 2004 - 2010" `
		-clsCompliant "false"

	Generate-Assembly-Info `
		-file "$base_dir\Raven.FileStorage\Properties\AssemblyInfo.cs" `
		-title "Raven Database Client $version" `
		-description "A linq enabled document database for .NET" `
		-company "Hibernating Rhinos" `
		-product "Raven Database $version" `
		-version $version `
		-copyright "Hibernating Rhinos & Ayende Rahien 2004 - 2010" `
		-clsCompliant "false"
		
	Generate-Assembly-Info `
		-file "$base_dir\Raven.Client.Tests\Properties\AssemblyInfo.cs" `
		-title "Raven Database Client $version" `
		-description "A linq enabled document database for .NET" `
		-company "Hibernating Rhinos" `
		-product "Raven Database $version" `
		-version $version `
		-copyright "Hibernating Rhinos & Ayende Rahien 2004 - 2010" `
		-clsCompliant "false"

	Generate-Assembly-Info `
		-file "$base_dir\Raven.Server\Properties\AssemblyInfo.cs" `
		-title "Raven Database $version" `
		-description "A linq enabled document database for .NET" `
		-company "Hibernating Rhinos" `
		-product "Raven Database $version" `
		-version $version `
		-copyright "Hibernating Rhinos & Ayende Rahien 2004 - 2010" `
		-clsCompliant "false"

		Generate-Assembly-Info `
		-file "$base_dir\Raven.Importer\Properties\AssemblyInfo.cs" `
		-title "Raven Database $version" `
		-description "A linq enabled document database for .NET" `
		-company "Hibernating Rhinos" `
		-product "Raven Database $version" `
		-version $version `
		-copyright "Hibernating Rhinos & Ayende Rahien 2004 - 2010" `
		-clsCompliant "false"

	Generate-Assembly-Info `
		-file "$base_dir\Raven.Scenarios\Properties\AssemblyInfo.cs" `
		-title "Raven Database $version" `
		-description "A linq enabled document database for .NET" `
		-company "Hibernating Rhinos" `
		-product "Raven Database $version" `
		-version $version `
		-copyright "Hibernating Rhinos & Ayende Rahien 2004 - 2010" `
		-clsCompliant "false"

	Generate-Assembly-Info `
		-file "$base_dir\Raven.Tests\Properties\AssemblyInfo.cs" `
		-title "Raven Database $version" `
		-description "A linq enabled document database for .NET" `
		-company "Hibernating Rhinos" `
		-product "Raven Database $version" `
		-version $version `
		-copyright "Hibernating Rhinos & Ayende Rahien 2004 - 2010" `
		-clsCompliant "false"

	Generate-Assembly-Info `
		-file "$base_dir\Raven.Importer\Properties\AssemblyInfo.cs" `
		-title "Raven Database $version" `
		-description "A linq enabled document database for .NET" `
		-company "Hibernating Rhinos" `
		-product "Raven Database $version" `
		-version $version `
		-copyright "Hibernating Rhinos & Ayende Rahien 2004 - 2010" `
		-clsCompliant "false"

	Generate-Assembly-Info `
		-file "$base_dir\Raven.Tryouts\Properties\AssemblyInfo.cs" `
		-title "Raven Database $version" `
		-description "A linq enabled document database for .NET" `
		-company "Hibernating Rhinos" `
		-product "Raven Database $version" `
		-version $version `
		-copyright "Hibernating Rhinos & Ayende Rahien 2004 - 2010" `
		-clsCompliant "false"
		
	#.\Utilities\Binaries\Raven.DefaultDatabase.Creator .\Raven.Server\Defaults\default.json
		
	new-item $release_dir -itemType directory
	new-item $buildartifacts_dir -itemType directory
}

task Compile -depends Init {
    exec C:\Windows\Microsoft.NET\Framework\v4.0.30128\MSBuild.exe """$sln_file"" /p:OutDir=""$buildartifacts_dir\"""
}

task Test -depends Compile {
  $old = pwd
  cd $build_dir
  exec "$tools_dir\xUnit\xunit.console.exe" "$build_dir\Raven.Tests.dll"
  exec "$tools_dir\xUnit\xunit.console.exe" "$build_dir\Raven.Scenarios.dll"
  #exec "$tools_dir\xUnit\xunit.console.exe" "$build_dir\Raven.Client.Tests.dll"
  cd $old
}

task Merge -depends Compile {
	$old = pwd
	cd $build_dir
	
	Remove-Item Raven.Server.Partial.exe -ErrorAction SilentlyContinue 
	Remove-Item Raven.Server.Partial.pdb -ErrorAction SilentlyContinue 
	Rename-Item $build_dir\Raven.Server.exe Raven.Server.Partial.exe
	Rename-Item $build_dir\Raven.Server.pdb Raven.Server.Partial.pdb
	
	& $tools_dir\ILMerge.exe Raven.Server.partial `
		$build_dir\Raven.Database.dll `
		$build_dir\Esent.Interop.dll `
		$build_dir\log4net.dll `
		$build_dir\Lucene.Net.dll `
		$build_dir\ICSharpCode.NRefactory.dll `
		$build_dir\Newtonsoft.Json.dll `
		/out:Raven.Server.exe `
		/t:exe 
		
	if ($lastExitCode -ne 0) {
        throw "Error: Failed to merge assemblies!"
    }
	cd $old
}

task Release -depends Test{
	& $tools_dir\zip.exe -9 -A -j `
		$release_dir\Raven.zip `
		$build_dir\Raven.Database.dll `
		$build_dir\Raven.Server.exe `
		$build_dir\Esent.Interop.dll `
		$build_dir\Esent.Interop.xml `
		$build_dir\log4net.dll `
		$build_dir\log4net.xml `
		$build_dir\Lucene.Net.dll `
		$build_dir\ICSharpCode.NRefactory.dll `
		$build_dir\Newtonsoft.Json.dll `
		license.txt `
		acknowledgements.txt
	if ($lastExitCode -ne 0) {
        throw "Error: Failed to execute ZIP command"
    }
}