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
$pkgver = "2.2.0"
$frameworks = "netstandard2.0"

write-progress "Building Release." "Running dotnet build..."
# Build the project in Release configuration
dotnet build nms-openwire.sln --configuration Release

if ($LASTEXITCODE -ne 0) {
    Write-Error "Build failed with exit code $LASTEXITCODE"
    exit $LASTEXITCODE
}

Write-Host "Release build completed successfully" -ForegroundColor Green

write-progress "Creating package directory." "Initializing..."
if (!(test-path package)) {
    mkdir package
}
else {
    # Clean package content if exists
    Remove-Item package\* -Recurse -Force
}

# Create a complete repository archive using git
write-progress "Creating complete repository archive." "Using git ls-files..."
$repoZipFile = "package/$pkgname-$pkgver-src.zip"

# Use git archive to create a zip of all tracked files
# This automatically excludes untracked files, build artifacts, and respects .gitignore
git archive --format=zip --output="$repoZipFile" HEAD

if ($LASTEXITCODE -eq 0) {
    Write-Host "Created repository archive: $repoZipFile" -ForegroundColor Green
} else {
    Write-Warning "Failed to create repository archive using git"
}

write-progress "Copying NuGet packages." "Scanning..."

if (test-path build) {
    Push-Location build

    $pkgdir = "..\package"
    
    # Create a temporary directory for the bin package
    $binDirectory = "$pkgname-$pkgver-bin"
    mkdir $binDirectory -ErrorAction SilentlyContinue
    
    $nupkg = "$pkgname.$pkgver.nupkg"
    Copy-Item -Path $nupkg -Destination $binDirectory

    $snupkg = "$pkgname.$pkgver.snupkg"
    Copy-Item -Path $snupkg -Destination $binDirectory
    
    Copy-Item ..\LICENSE.txt -Destination $binDirectory
    Copy-Item ..\NOTICE.txt -Destination $binDirectory
    
    # Create the bin zip file
    $binZipFile = "$pkgdir\$pkgname-$pkgver-bin.zip"
    Compress-Archive -Path $binDirectory -DestinationPath $binZipFile -Force
    
    Remove-Item $binDirectory -Recurse -Force

    Write-Host "Created binary package with NuGet files: $binZipFile" -ForegroundColor Green

    Pop-Location
}

write-progress -Completed "Packaging" "Complete."