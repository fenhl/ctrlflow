#!/usr/bin/env pwsh

cargo check
if (-not $?)
{
    throw 'Native Failure'
}

# copy the tree to the WSL file system to improve compile times
wsl rsync --delete -av /mnt/c/Users/fenhl/git/github.com/fenhl/ctrlflow/stage/ /home/fenhl/wslgit/github.com/fenhl/ctrlflow/ --exclude target
if (-not $?)
{
    throw 'Native Failure'
}

wsl env -C /home/fenhl/wslgit/github.com/fenhl/ctrlflow cargo check --all-features
if (-not $?)
{
    throw 'Native Failure'
}
