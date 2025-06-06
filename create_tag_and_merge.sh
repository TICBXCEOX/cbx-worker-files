#!/bin/bash
# comando: bash create_tag_and_merge.sh

set -e
set -o pipefail
#set -x  # ← Ativa o modo debug (imprime cada linha executada)

# === Configurações iniciais ===
BRANCH_MAIN="main"
VERSION_FILE="version.txt"
KEY_FILE="key.py"

# === Inicia versão se não existir ===
if [ ! -f "$VERSION_FILE" ]; then
  echo "2.0.0" > "$VERSION_FILE"
  echo "📄 Criado arquivo $VERSION_FILE com versão inicial 2.0.0"
fi

# === Função para incrementar PATCH ===
increment_patch() {
  VERSION=$(cat "$VERSION_FILE")

  MAJOR=$(echo "$VERSION" | cut -d. -f1)
  MINOR=$(echo "$VERSION" | cut -d. -f2)
  PATCH=$(echo "$VERSION" | cut -d. -f3)

  # fallback para zero, caso algo venha vazio
  MAJOR=${MAJOR:-0}
  MINOR=${MINOR:-0}
  PATCH=${PATCH:-0}

  ((PATCH++))
  NEW_VERSION="$MAJOR.$MINOR.$PATCH"

  echo "$NEW_VERSION" > "$VERSION_FILE"
  echo "$NEW_VERSION"
}

# === Disclaimer e confirmação ===
echo "⚠️ ATENÇÃO: Este script realiza operações críticas no repositório."
echo "Deseja continuar com a operação? (sim/nao)"
read PROSSEGUIR_CONFIRMADO
if [ "$PROSSEGUIR_CONFIRMADO" != "sim" ]; then
  echo "🚫 Operação cancelada. Saindo..."
  exit 0
fi

# === Atualização de versão ===
NEW_VERSION=$(increment_patch)
echo "🔖 Versão atualizada para $NEW_VERSION"

# Atualiza o key.py
echo "APP_VERSION = \"$NEW_VERSION\"" > $KEY_FILE
echo "✅ Arquivo $KEY_FILE atualizado com a versão"

# === Geração da tag ===
TAG_TIME=$(date +"%Y%m%d-%H%M")
TAG_NAME="v$NEW_VERSION-$TAG_TIME"

git checkout $BRANCH_MAIN
git pull origin $BRANCH_MAIN

git add $VERSION_FILE $KEY_FILE
git commit -m "🔖 Atualiza versão para $NEW_VERSION"
git tag -a "$TAG_NAME" -m "Versão $NEW_VERSION gerada em $TAG_TIME"
git push origin "$TAG_NAME"

echo "✅ Tag '$TAG_NAME' criada e enviada com sucesso!"

# === Merge ou push ===
echo ""
echo "Deseja seguir com o merge de uma branch develop/hotfix para a '$BRANCH_MAIN' ou apenas fazer push da '$BRANCH_MAIN'?"
echo "Digite 'merge' para fazer o merge, ou 'push' para apenas enviar a main ao origin."
read -p "(merge/push): " ACAO

if [ "$ACAO" = "merge" ]; then
  read -p "Digite o nome da branch para fazer o merge com a '$BRANCH_MAIN': " BRANCH_HOTFIX
  if [ -z "$BRANCH_HOTFIX" ]; then
    echo "🚫 Nenhuma branch hotfix informada. Saindo..."
    exit 1
  fi

  git merge "$BRANCH_HOTFIX" || {
    echo "⚠️ Conflitos encontrados. Resolva manualmente."
    exit 1
  }

  git push origin $BRANCH_MAIN
  echo "✅ Merge concluído com sucesso!"

elif [ "$ACAO" = "push" ]; then
  git push origin $BRANCH_MAIN
  echo "✅ Push da '$BRANCH_MAIN' concluído com sucesso!"
else
  echo "🚫 Opção inválida."
  exit 1
fi