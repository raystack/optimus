# Shell Autocompletion Feature

The shell auto-completion feature in optimus CLI provide the relief you not to remember the specific command , you just need to type some initials of the command then complete command gets generated automatically.If a completion command is already provided, uses that instead.

## Steps to setup shell completion functionality 

Before moving towards setup one can check whether the functionality is enabled in it's Optimus binary by executing `optimus [tab][tab]` in the CLI. If it's working fine then you are good to go.
Otherwise, follow the setup shown below.

### 1. Bash auto-completion 

### Introduction :
The optimus completion script for Bash can be generated with `./optimus completion bash`. Sourcing this script in your shell enables optimus completion.

However, the completion script depends on bash-completion, which means that you have to install this software first (you can test if you have bash-completion already installed by running 
`type _init_completion`).


>Warning: There are two versions of bash-completion, v1 and v2. V1 is for Bash 3.2 (which is the default on macOS), and v2 is for Bash 4.1+. The optimus completion script doesn't work correctly with bash-completion v1 and Bash 3.2. It requires bash-completion v2 and Bash 4.1+. Thus, to be able to correctly use optimus completion on macOS, you have to install and use Bash 4.1+ (instructions). The following instructions assume that you use Bash 4.1+ (that is, any Bash version of 4.1 or newer).

### Enable optimus autocompletion
You now have to ensure that the optimus completion script gets sourced in all your shell sessions. There are multiple ways to achieve this:
- Source the completion script in your ~/.bash_profile file:

```
echo 'source <(./optimus completion bash)' >> ~/.bash_profile
```

- Add the completion script to the /usr/local/etc/bash_completion.d directory:
```
# To load completions for each session, execute once:
# Linux:
$ ./optimus completion bash > /etc/bash_completion.d/_optimus
# macOS:
$ ./optimus completion bash > /usr/local/etc/bash_completion.d/_optimus
```

- If you installed optimus with Homebrew (as explained here), then the optimus completion script should already be in /usr/local/etc/bash_completion.d/_optimus. In that case, you don't need to do anything.

>Note: The Homebrew installation of bash-completion v2 sources all the files in the BASH_COMPLETION_COMPAT_DIR directory, that's why the latter two methods work.

In any case, after reloading your shell, optimus completion should be working.

### 2. Zsh Auto-completion

The optimus completion script for Zsh can be generated with the command `./optimus completion zsh`. Sourcing the completion script in your shell enables optimus autocompletion.

- If shell completion is not already enabled in your environment,you will need to enable it.You can execute the following once:
If you get an error like complete:13: command not found: compdef, then add the following to the beginning of your ~/.zshrc file:
```
  $ echo "autoload -U compinit; compinit" >> ~/.zshrc
```
- To load completions for each session, execute once:
```
  $ ./optimus completion zsh > "${fpath[1]}/_optimus"
```
- Now start a new shell for this setup to take effect and execute the below command to do sourcing in all your shell session:
```
  $ source ~/.zshrc 
```

After setup is completed
```
 # Run the following command in shell (bash/zsh)
 $ ./optimus [tab][tab]
 ```

Output :
 ```
$ ./optimus 
config   -- Manage optimus configuration required to deploy specifications
create   -- Create a new job/resource
deploy   -- Deploy current project to server
help     -- Help about any command
render   -- convert raw representation of specification to consumables
replay   -- re-running jobs in order to update data for older dates/partitions
serve    -- Starts optimus service
version  -- Print the client version information
 ```

## Additional Features :

You can configure the command auto creation:

- To tell Optimus not to provide the default completion command:
```
rootCmd.CompletionOptions.DisableDefaultCmd = true
```

- To tell Optimus not to provide the user with the --no-descriptions flag to the completion sub-commands:
```
rootCmd.CompletionOptions.DisableNoDescFlag = true
```

- To tell Optimus to completely disable descriptions for completions:
```
rootCmd.CompletionOptions.DisableDescriptions = true
```