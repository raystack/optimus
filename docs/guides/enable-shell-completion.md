# Shell Autocompletion Feature

The shell autcompletion feature in optimus CLI is supported by one of the popular CLI framework for Go i.e Cobra, which contains a library for creating powerful modern CLI applications and a tool to rapidly generate Cobra based applications and command files.
The cobra version v1.2.1 which enables the following functionality in Optimus CLI :-
Automatically adds completion command for shell completions i.e bash, zsh, fish and PowerShell. If a completion command is already provided, uses that instead.

## Steps to setup shell completion functionality 

Before moving towards setup one can check whether the functionality is enabled in it Optimus binary by executing `optimus [tab][tab]` in the optimus CLI. If it's working fine then you are good to go.
Otherwise, follow the setup shown below.
```
Bash:

  $ source <(./optimus completion bash)

  # To load completions for each session, execute once:
  # Linux:
  $ ./optimus completion bash > /etc/bash_completion.d/./optimus
  # macOS:
  $ ./optimus completion bash > /usr/local/etc/bash_completion.d/./optimus

Zsh:

  # If shell completion is not already enabled in your environment,you will need to enable it.
  # You can execute the following once:

  $ echo "autoload -U compinit; compinit" >> ~/.zshrc

  # To load completions for each session, execute once:
  $ ./optimus completion zsh > "${fpath[1]}/_optimus"

  # Now start a new shell for this setup to take effect and execute the below command :
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