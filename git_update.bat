
@echo off
setlocal

REM ===========================================================
REM Git Commit & Push (normal) -> sempre na branch main
REM Uso:
REM   git-commit-push-main.bat ["Mensagem do commit"]
REM ===========================================================

REM 0) Verifica Git
git --version >nul 2>&1
if errorlevel 1 (
  echo [ERRO] Git nao encontrado no PATH. Instale o Git e tente novamente.
  exit /b 1
)

REM 1) Verifica repo
git rev-parse --is-inside-work-tree >nul 2>&1
if errorlevel 1 (
  echo [ERRO] Este diretorio nao e um repositorio Git.
  exit /b 1
)

REM 2) Garante que estamos na main
echo [STEP] Trocando para branch 'main'...
git checkout main
if errorlevel 1 (
  echo [ERRO] Nao foi possivel trocar para 'main'. Crie ou configure a branch.
  exit /b 1
)

REM 3) Mensagem de commit
set COMMIT_MSG=%~1
if "%COMMIT_MSG%"=="" (
  set /p COMMIT_MSG=Digite a mensagem do commit: 
)

REM 4) Adicionar tudo
echo [STEP] Adicionando todas as mudancas...
git add -A
if errorlevel 1 (
  echo [ERRO] Falha ao adicionar arquivos.
  exit /b 1
)

REM 5) Commit
echo [STEP] Criando commit...
git commit -m "%COMMIT_MSG%"
if errorlevel 1 (
  echo [ERRO] Falha ao criar commit (talvez nao haja mudancas?).
  exit /b 1
)

REM 6) Push na main (configura upstream na primeira vez)
echo [STEP] Enviando para 'origin/main'...
git rev-parse --symbolic-full-name --abbrev-ref @{u} >nul 2>&1
if errorlevel 1 (
  git push -u origin main
) else (
  git push origin main
)

if errorlevel 1 (
  echo [ERRO] Falha no push para origin/main.
  exit /b 1
)

echoecho [OK] Commit e push realizados com sucesso na 'main'.
