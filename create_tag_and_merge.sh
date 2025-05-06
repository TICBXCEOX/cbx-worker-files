#!/bin/bash

# Garante que o script pare em qualquer erro
set -e

# Aviso importante - Disclaimer
echo "⚠️ ATENÇÃO: Este script realiza operações críticas no repositório."
echo "    - Criação de uma nova tag na branch 'main'."
echo "    - Possível merge de uma branch hotfix, o que pode afetar o código."
echo "    - Caso haja conflitos durante o merge, você precisará resolvê-los manualmente."
#echo "    - Após o merge, a branch hotfix será deletada local e remotamente."
echo ""
echo "   **Certifique-se de que está ciente dos riscos antes de prosseguir.**"
echo "   **Se não souber o que está fazendo, por favor, interrompa este script.**"
echo ""

# Confirmação de prosseguimento com o disclaimer
echo "Deseja continuar com a operação? (sim/nao)"
read PROSSEGUIR_CONFIRMADO

# Verifica a resposta
if [ "$PROSSEGUIR_CONFIRMADO" != "sim" ]; then
  echo "🚫 Operação cancelada. Saindo..."
  exit 0
fi

# Define o nome da branch principal
BRANCH_MAIN="main"

# Criação de Tag - Obtendo o timestamp para nome da tag
TAG_TIME=$(date +"%Y%m%d-%H%M")
TAG_NAME="stable-$TAG_TIME"

echo "📌 Criando tag '$TAG_NAME' a partir da branch '$BRANCH_MAIN'..."

# Vai para a branch principal 'main'
git checkout $BRANCH_MAIN
git pull origin $BRANCH_MAIN

# Cria a tag
git tag -a "$TAG_NAME" -m "Versão estável gerada automaticamente em $TAG_TIME"

# Envia a tag para o repositório remoto
git push origin "$TAG_NAME"

echo "✅ Tag '$TAG_NAME' criada e enviada com sucesso!"

# Escolha entre merge ou apenas push da main
echo ""
echo "Deseja seguir com o merge de uma branch hotfix para a '$BRANCH_MAIN' ou apenas fazer push da '$BRANCH_MAIN'?"
echo "Digite 'merge' para fazer o merge, ou 'push' para apenas enviar a main ao origin."
read -p "(merge/push): " ACAO

if [ "$ACAO" = "merge" ]; then
  read -p "Digite o nome da branch para fazer o merge com a '$BRANCH_MAIN' (ex: hotfix_bla_bla_bla): " BRANCH_HOTFIX

  if [ -z "$BRANCH_HOTFIX" ]; then
    echo "🚫 Nenhuma branch hotfix informada. Saindo..."
    exit 1
  fi

  echo "🔄 Iniciando merge da branch '$BRANCH_HOTFIX' na branch '$BRANCH_MAIN'..."

  git merge "$BRANCH_HOTFIX" || {
      echo "⚠️ Conflitos encontrados durante o merge. Resolva os conflitos manualmente e depois execute:"
      echo "git add ."
      echo "git commit -m 'Resolvendo conflitos no merge da branch $BRANCH_HOTFIX'"
      exit 1
  }

  git push origin $BRANCH_MAIN
  echo "✅ Merge concluído e alterações enviadas com sucesso!"

elif [ "$ACAO" = "push" ]; then
  echo "🚀 Enviando a branch '$BRANCH_MAIN' atualizada para o repositório remoto..."
  git push origin $BRANCH_MAIN
  echo "✅ Push da '$BRANCH_MAIN' concluído com sucesso!"

else
  echo "🚫 Opção inválida. Saindo sem realizar merge nem push da main."
  exit 1
fi