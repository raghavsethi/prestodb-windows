@echo off
@rem
@rem skydrill - makeall.cmd
@rem
setlocal enabledelayedexpansion
set clean=%1 
set build_home=%~dp0

call gradlew %clean% makeTarget 