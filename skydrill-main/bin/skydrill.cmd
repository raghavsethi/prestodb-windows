@echo off
@rem /*
@rem * Licensed under the Apache License, Version 2.0 (the "License");
@rem * you may not use this file except in compliance with the License.
@rem * You may obtain a copy of the License at
@rem *
@rem *     http://www.apache.org/licenses/LICENSE-2.0
@rem *
@rem * Unless required by applicable law or agreed to in writing, software
@rem * distributed under the License is distributed on an "AS IS" BASIS,
@rem * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@rem * See the License for the specific language governing permissions and
@rem * limitations under the License.
@rem */
@rem skydrill command script
@rem
@rem Environment Variables
@rem
@rem   JAVA_HOME        The java implementation to use.  Overrides JAVA_HOME.
@rem
@rem   SKYDRILL_CLASSPATH Extra Java CLASSPATH entries.
@rem
@rem   SKYDRILL_HEAPSIZE  The maximum amount of heap to use, in MB.
@rem                    Default is 1000.
@rem
@rem   SKYDRILL_OPTS      Extra Java runtime options.
@rem
@rem   SKYDRILL_CONF_DIR  Alternate conf dir. Default is ${SKYDRILL_HOME}/etc.
@rem
@rem   SKYDRILL_ROOT_LOGGER The root appender. Default is INFO,console
@rem

:main
  setlocal enabledelayedexpansion

  call %~dp0skydrill-config.cmd

  if defined DEBUG_PORT (
    set DEBUG_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,address=%DEBUG_PORT%,suspend=n
  )

  set SKYDRILL-command=%1
  if not defined SKYDRILL-command (
      goto print_usage
  )

  call :make_command_arguments %*

  set shellcommands=classpath help version
  for %%i in ( %shellcommands% ) do (
    if %SKYDRILL-command% == %%i set shellcommand=true
  )
  if defined shellcommand (
    call :%SKYDRILL-command% %*
    goto :eof
  )

  set corecommands=server cli zookeeper dfs
  for %%i in ( %corecommands% ) do (
    if %SKYDRILL-command% == %%i set corecommand=true  
  )
  if defined corecommand (
    call :%SKYDRILL-command% %SKYDRILL-command-arguments%
  ) else (
    set CLASS=%SKYDRILL-command%
  )

  set path=%SKYDRILL_BIN_DIR%;%SKYDRILL_SBIN_DIR%;%windir%\system32;%windir%
  call %JAVA% %DEBUG_OPTS% %JAVA_HEAP_MAX% %SKYDRILL_OPTS% %CLASS% %SKYDRILL-command-arguments%
  goto :eof

:server
  set CLASS=io.panyu.skydrill.server.SkydrillServer
  set SKYDRILL_OPTS=%SKYDRILL_SERVER_OPTS% %SKYDRILL_OPTS%
  goto :eof

:classpath
  echo %CLASSPATH% 
  goto :eof

:help
  call :print_usage
  goto :eof

:cli
  set CLASSPATH=%SKYDRILL_BIN_DIR%\cli\*
  set CLASS=com.facebook.presto.cli.Presto
  set SKYDRILL_OPTS=%SKYDRILL_CLIENT_OPTS% %SKYDRILL_OPTS% 
  goto :eof

:dfs
  set CLASSPATH=%SKYDRILL_HOME%\plugin\hive\*
  set CLASS=org.apache.hadoop.fs.FsShell
  set SKYDRILL_OPTS=%SKYDRILL_CLIENT_OPTS% %SKYDRILL_OPTS% 
  goto :eof

:zookeeper
  set CLASS=org.apache.zookeeper.server.quorum.QuorumPeerMain
  set ZOOKEEPER_OPTS=-XX:+UseG1GC -XX:MaxGCPauseMillis=10 -XX:-PrintGC -Dlog4j.configuration=%LOG4J_CONF_FILE% -Dcom.sun.management.jmxremote.port=%ZOOKEEPER_JMX_PORT%
  set SKYDRILL_OPTS=%SKYDRILL_SERVER_OPT% %ZOOKEEPER_OPTS% %SKYDRILL_OPTS%
  goto :eof

:version
  type RELEASE
  goto :eof

:make_command_arguments
  if "%2" == "" goto :eof
  set _count=0
  set _shift=1
  for %%i in (%*) do (
    set /a _count=!_count!+1
    if !_count! GTR %_shift% ( 
	if not defined _arguments (
	  set _arguments=%%i
	) else (
          set _arguments=!_arguments! %%i
	)
    )
  )
  set SKYDRILL-command-arguments=%_arguments%
  goto :eof

:print_usage
  @echo Usage: SKYDRILL COMMAND
  @echo where COMMAND is one of:
  @echo   help
  @echo.
  @echo   server               launch presto server
  @echo.
  @echo   cli                  launch presto shell
  @echo   version              print the version
  @echo.
  @echo   zookeeper            launch zookeeper daemon
  @echo.
  @echo  or
  @echo   CLASSNAME            run the class named CLASSNAME
  @echo Most commands print help when invoked w/o parameters.

endlocal