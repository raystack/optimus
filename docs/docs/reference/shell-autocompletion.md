---
id: shell-autocompletion
title: Shell autocompletion
---
### Bash auto-completion 

The optimus completion script for Bash can be generated with `optimus completion bash`. Sourcing this script in your shell enables optimus completion.

However, the completion script depends on bash-completion, which means that you have to install this software first (you can test if you have bash-completion already installed by running 
`type _init_completion`).


>Warning: There are two versions of bash-completion, v1 and v2. V1 is for Bash 3.2 (which is the default on macOS), and v2 is for Bash 4.1+. The optimus completion script doesn't work correctly with bash-completion v1 and Bash 3.2. It requires bash-completion v2 and Bash 4.1+. Thus, to be able to correctly use optimus completion on macOS, you have to install and use Bash 4.1+ (instructions). The following instructions assume that you use Bash 4.1+ (that is, any Bash version of 4.1 or newer).


You now have to ensure that the optimus completion script gets sourced in all your shell sessions. There are multiple ways to achieve this:
- Source the completion script in your ~/.bash_profile file:

```
echo 'source <(optimus completion bash)' >> ~/.bash_profile
```

- Add the completion script to the /usr/local/etc/bash_completion.d directory:
```
# To load completions for each session, execute once:
# Linux:
$ optimus completion bash > /etc/bash_completion.d/_optimus
# macOS:
$ optimus completion bash > /usr/local/etc/bash_completion.d/_optimus
```

- If you installed optimus with Homebrew (as explained in [getting started](../getting-started/installation.md)), then the optimus completion script should already be in /usr/local/etc/bash_completion.d/_optimus. In that case, you don't need to do anything.

>Note: The Homebrew installation of bash-completion v2 sources all the files in the BASH_COMPLETION_COMPAT_DIR directory, that's why the latter two methods work.

In any case, after reloading your shell, optimus completion should be working.


### Zsh Auto-completion

The optimus completion script for Zsh can be generated with the command `optimus completion zsh`. Sourcing the completion script in your shell enables optimus autocompletion.

- If shell completion is not already enabled in your environment, you will need to enable it. You can execute the following once:

>If you get an error like `complete:13: command not found: compdef`, then add the following to the beginning of your `~/.zshrc` file:

```
  $ echo "autoload -U compinit; compinit" >> ~/.zshrc
```
- To load completions for each session, execute once:
```
  $ optimus completion zsh > "${fpath[1]}/_optimus"
```
- Now start a new shell for this setup to take effect and execute the below command to do sourcing in all your shell session:
```
  $ source ~/.zshrc 
```

After setup is completed
```
 # Run the following command in shell (bash/zsh)
 $ optimus [tab][tab]
```

Output :
 ```
$ optimus 
Optimus is an easy-to-use, reliable, and performant workflow orchestrator for
data transformation, data modeling, pipelines, and data quality management.

For passing authentication header, set one of the following environment
variables:
1. OPTIMUS_AUTH_BASIC_TOKEN
2. OPTIMUS_AUTH_BEARER_TOKEN

USAGE
  optimus <command> <subcommand> [flags]

CORE COMMANDS
  backup      Backup a resource and its downstream
  deploy      Deploy current optimus project to server
  job         Interact with schedulable Job
  replay      Re-running jobs in order to update data for older dates/partitions
  resource    Interact with data resource

DEV COMMANDS
  serve       Starts optimus service

ADDITIONAL COMMANDS
  completion  generate the autocompletion script for the specified shell
  config      Manage optimus configuration required to deploy specifications
  extension   Operate with extension
  help        Help about any command
  version     Print the client version information

FLAGS
  --help       Show help for command
  --no-color   Disable colored output

EXAMPLES
  $ optimus job create
  $ optimus backup create
  $ optimus backup list
  $ optimus replay create

LEARN MORE
  Use 'optimus <command> <subcommand> --help' for more information about a command.
  Read the manual at https://goto.github.io/optimus/

FEEDBACK
  Open an issue here https://github.com/goto/optimus/issues
 ```