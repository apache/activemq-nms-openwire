# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

$pkgname = "Apache.NMS.ActiveMQ"
$pkgver = "2.1.0"
$frameworks = "netstandard2.0"

write-progress "Creating package directory." "Initializing..."
if (!(test-path package)) {
    mkdir package
}
else {
    # Clean package content if exists
    Remove-Item package\* -Recurse
}

if (test-path build) {
    Push-Location build

    $pkgdir = "..\package"

    write-progress "Packaging Application files." "Scanning..."
    $zipfile = "$pkgdir\$pkgname-$pkgver-bin.zip"

    $directory = "$pkgname-$pkgver-bin"
    mkdir $directory

    Copy-Item ..\LICENSE.txt -Destination $directory
    Copy-Item ..\NOTICE.txt -Destination $directory
    
    # clean up temp
    Remove-Item temp -Recurse -ErrorAction Ignore

    foreach ($framework in $frameworks) {
        Copy-Item $framework -Destination temp\$framework -Recurse

        # clean up third party binaries
        Get-ChildItem temp -File -Exclude "*Apache.NMS*" -Recurse | Remove-Item -Recurse

        Copy-Item -Path "temp\$framework" -Destination $directory -Recurse
    }

    Compress-Archive -Path $directory -Update -DestinationPath $zipfile

    Remove-Item $directory -Recurse -ErrorAction Ignore
    
    $nupkg = "$pkgname.$pkgver.nupkg"
    $nupkgdestination = "$pkgdir\$nupkg"
    Copy-Item -Path $nupkg -Destination $nupkgdestination

    $snupkg = "$pkgname.$pkgver.snupkg"
    $snupkgdestination = "$pkgdir\$snupkg"
    Copy-Item -Path $snupkg -Destination $snupkgdestination

    # clean up temp
    Remove-Item temp -Recurse -ErrorAction Inquire

    Pop-Location
}

write-progress "Packaging Source code files." "Scanning..."
$pkgdir = "package"
$zipfile = "$pkgdir\$pkgname-$pkgver-src.zip"

$directory = "$pkgname-$pkgver-src"
mkdir $directory

# clean temp dir if exists
Remove-Item temp -Recurse -ErrorAction Ignore

# copy files to temp dir
Copy-Item src -Destination "$directory\src" -Recurse
Copy-Item test -Destination "$directory\test" -Recurse

# clean up debug artifacts if there are any
Get-ChildItem $directory -Include bin, obj -Recurse | Remove-Item -Recurse

Copy-Item .\LICENSE.txt -Destination $directory
Copy-Item .\NOTICE.txt -Destination $directory
Copy-Item .\keyfile -Destination $directory -Recurse
Copy-Item .\nms-openwire.sln -Destination $directory
Copy-Item .\package.ps1 -Destination $directory

Compress-Archive -Path $directory -DestinationPath $zipfile

Remove-Item $directory -Recurse -ErrorAction Ignore

write-progress "Packaging Docs"
$zipfile = "$pkgdir\$pkgname-$pkgver-docs.zip"
$directory = "$pkgname-$pkgver-docs"
mkdir $directory

Copy-Item -Path .\docs\_site\* -Destination $directory -Recurse
Copy-Item .\LICENSE.txt -Destination $directory
Copy-Item .\NOTICE.txt -Destination $directory

Compress-Archive -Path $directory -DestinationPath $zipfile

Remove-Item $directory -Recurse -ErrorAction Ignore

write-progress -Completed "Packaging" "Complete."