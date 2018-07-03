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

set SKYDRILL_HOME=%~dp0
for %%i in (%SKYDRILL_HOME%.) do (
  set SKYDRILL_HOME=%%~dpi
)
if "%SKYDRILL_HOME:~-1%" == "\" (
  set SKYDRILL_HOME=%SKYDRILL_HOME:~0,-1%
)

if not exist %SKYDRILL_HOME%\lib\skydrill*.jar (
    @echo +================================================================+
    @echo ^|      Error: SKYDRILL_HOME is not set correctly                   ^|
    @echo +----------------------------------------------------------------+
    @echo ^| Please set your SKYDRILL_HOME variable to the absolute path of   ^|
    @echo ^| the directory that contains the SKYDRILL distribution      ^|
    @echo +================================================================+
    exit /b 1
)

set SKYDRILL_BIN_DIR=%SKYDRILL_HOME%\bin
set SKYDRILL_SBIN_DIR=%SKYDRILL_HOME%\sbin

if not defined SKYDRILL_CONF_DIR (
  set SKYDRILL_CONF_DIR=%SKYDRILL_HOME%\etc
)

@rem
@rem setup java environment variables
@rem

if not defined JAVA_HOME (
  set JAVA_HOME=c:\tempest\tools\java\jdk1.8.0_121
)

if not exist %JAVA_HOME%\bin\java.exe (
  echo Error: JAVA_HOME is incorrectly set.
  goto :eof
)

set JAVA=%JAVA_HOME%\bin\java
set JAVA_HEAP_MAX=-Xmx1024m

@rem
@rem check envvars which might override default args
@rem

if defined SKYDRILL_HEAPSIZE (
  set JAVA_HEAP_MAX=-Xmx%SKYDRILL_HEAPSIZE%m
)

@rem
@rem CLASSPATH initially contains %SKYDRILL_CONF_DIR%
@rem

set CLASSPATH=%SKYDRILL_HOME%\*;%SKYDRILL_CONF_DIR%
set CLASSPATH=%CLASSPATH%;%JAVA_HOME%\lib\tools.jar

@rem
@rem add libs to CLASSPATH
@rem

set CLASSPATH=!CLASSPATH!;%SKYDRILL_HOME%\sbin\*
set CLASSPATH=!CLASSPATH!;%SKYDRILL_HOME%\lib\*

if not defined SKYDRILL_DATA_DIR (
  set SKYDRILL_DATA_DIR=%SKYDRILL_HOME%\var\data
)

if not defined SKYDRILL_LOG_DIR (
  set SKYDRILL_LOG_DIR=%SKYDRILL_HOME%\var\log
)

if not defined SKYDRILL_LOG_FILE (
  set SKYDRILL_LOG_FILE=%SKYDRILL_LOG_DIR%\skydrill.log
)

if not defined SKYDRILL_CONF_FILE (
  set SKYDRILL_CONF_FILE=%SKYDRILL_CONF_DIR%\config.properties 
)

if not defined SKYDRILL_LOG_CONF_FILE (
  set SKYDRILL_LOG_CONF_FILE=%SKYDRILL_CONF_DIR%\log.properties
)

if not defined LOG4J_CONF_FILE (
  set LOG4J_CONF_FILE=zookeeper.log.properties
)

if not defined ZOOKEEPER_JMX_PORT (
  set ZOOKEEPER_JMX_PORT=9990
)

set SKYDRILL_OPTS=-Dskydrill.home=%SKYDRILL_HOME% -Djava.library.path=%SKYDRILL_SBIN_DIR%
set SKYDRILL_OPTS=%SKYDRILL_OPTS% -Dlog.output-file=%SKYDRILL_LOG_FILE%
set SKYDRILL_OPTS=%SKYDRILL_OPTS% -Dnode.data-dir=%SKYDRILL_DATA_DIR%
set SKYDRILL_OPTS=%SKYDRILL_OPTS% -Dlog.levels-file=%SKYDRILL_LOG_CONF_FILE%
set SKYDRILL_OPTS=%SKYDRILL_OPTS% -Dconfig=%SKYDRILL_CONF_FILE%
set SKYDRILL_OPTS=%SKYDRILL_OPTS% -Dcom.sun.management.jmxremote
set SKYDRILL_OPTS=%SKYDRILL_OPTS% -Dcom.sun.management.jmxremote.authenticate=false
set SKYDRILL_OPTS=%SKYDRILL_OPTS% -Dcom.sun.management.jmxremote.ssl=false
set SKYDRILL_OPTS=%SKYDRILL_OPTS% -Djdk.net.useFastTcpLoopback=true
set SKYDRILL_OPTS=%SKYDRILL_OPTS% -Djdk.net.enableFastFileTransfer=true

if not defined SKYDRILL_SERVER_OPTS (
  set SKYDRILL_SERVER_OPTS=-server -XX:+UseG1GC -XX:G1HeapRegionSize=32M -XX:+UseGCOverheadLimit -XX:+ExplicitGCInvokesConcurrent -XX:+HeapDumpOnOutOfMemoryError -XX:+ExitOnOutOfMemoryError 
)

if not defined SKYDRILL_CLIENT_OPTS (
  set SKYDRILL_CLIENT_OPTS=-client -XX:+UseG1GC -XX:G1HeapRegionSize=32M -XX:+UseGCOverheadLimit -XX:+ExplicitGCInvokesConcurrent -XX:+HeapDumpOnOutOfMemoryError -XX:+ExitOnOutOfMemoryError 
)

:eof 