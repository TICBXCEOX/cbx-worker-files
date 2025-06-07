#!/bin/bash
# comando: bash create_tag_and_merge.sh

set -e
set -o pipefail
#set -x  # Ativa o modo debug (imprime cada linha executada)

# === Configurações iniciais ===
BRANCH_MAIN="main"
VERSION_FILE="src/version/version.txt"
KEY_FILE="src/version/key.py"
ENV_FILE="src/version/env.py"

# === Disclaimer e confirmação ===
echo "⚠️ ATENÇÃO: Este script realiza operações críticas no repositório."
echo "Deseja continuar com a operação? (s/n)"
read PROSSEGUIR_CONFIRMADO
if [ "$PROSSEGUIR_CONFIRMADO" != "s" ]; then
  echo "🚫 Operação cancelada. Saindo..."
  exit 0
fi



# =====================
# === CHECKOUT MAIN ===
# =====================
git checkout $BRANCH_MAIN
git pull origin $BRANCH_MAIN
echo "VOCÊ ESTA NA BRANCH: $BRANCH_MAIN"

# === Função para incrementar PATCH ===
increment_patch() {
  if [ ! -f "$VERSION_FILE" ]; then
    echo "2.0.0" > "$VERSION_FILE"
    echo "📄 Criado arquivo $VERSION_FILE com versão inicial 2.0.0"
  fi

  VERSION=$(cat "$VERSION_FILE")

  MAJOR=$(echo "$VERSION" | cut -d. -f1)
  MINOR=$(echo "$VERSION" | cut -d. -f2)
  PATCH=$(echo "$VERSION" | cut -d. -f3)

  MAJOR=${MAJOR:-0}
  MINOR=${MINOR:-0}
  PATCH=${PATCH:-0}

  ((PATCH++))
  NEW_VERSION="$MAJOR.$MINOR.$PATCH"

  echo "$VERSION $NEW_VERSION"
}

# === Atualização de versão ===
read VERSION NEW_VERSION <<< "$(increment_patch)"
echo "Versão atual: $VERSION"
echo "Nova sugestão de versão: $NEW_VERSION"

#read -p "Deseja alterar a nova versão $NEW_VERSION? (pressione ENTER para manter ou digite nova versão): " USER_INPUT
read -e -p "Confirme a nova versão sugerida com ENTER ou altere: " -i "$NEW_VERSION" USER_INPUT

if [[ -n "$USER_INPUT" ]]; then
  NEW_VERSION="$USER_INPUT"
fi

echo "📝 Versão a ser usada: $NEW_VERSION"
read -p "Confirma essa versão? (s/n): " CONFIRM

if [[ "$CONFIRM" != "s" && "$CONFIRM" != "S" ]]; then
  echo "🚫 Operação cancelada. Nenhuma alteração feita."
  exit 1 #sai do script
fi

echo "$NEW_VERSION" > "$VERSION_FILE"
echo "🚀 Versão final definida: $NEW_VERSION"

# === Geração da tag ===
TAG_TIME=$(date +"%Y%m%d-%H%M")
TAG_NAME="v$NEW_VERSION-$TAG_TIME"
git tag -a "$TAG_NAME" -m "Versão $NEW_VERSION gerada em $TAG_TIME"
git push origin "$TAG_NAME"
echo "✅ Tag '$TAG_NAME' criada e enviada com sucesso!"

# === Atualiza Key.py ('versão' usado no código) ===
echo "APP_VERSION = \"$NEW_VERSION\"" > $KEY_FILE
echo "✅ Arquivo $KEY_FILE atualizado com a nova versão $NEW_VERSION"

git add $VERSION_FILE $KEY_FILE
git commit -m "🔖 Atualizado versão para $NEW_VERSION"

# === Atualização environment para produção ===
echo "ENV='production'" > $ENV_FILE
echo "✅ Arquivo $ENV_FILE da $BRANCH_MAIN atualizado para produção"

git add $ENV_FILE
git commit -m "🔖 Atualizado env para produção"

# === Merge ou push ===
echo ""
echo "Deseja fazer merge de uma branch develop/hotfix para a '$BRANCH_MAIN' ou apenas fazer push da '$BRANCH_MAIN'?"
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