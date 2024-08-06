version=':v1.00' # Versão Inicial

# echo "TROCAR A VERSAO DAS IMAGENS EM docker-compose.yml"
# sed -i'' -e 's/:v1.00/:v4.00/g' docker-compose.yml

echo "GERAR IMAGENS SG_Integração_Welt ${version}"

# echo "CLASSES ACESSORIAS Logger Exchange e Configurator"
# if [ -d "utils" ]; then
#   rm -rf utils
# fi
# sleep 5
# git clone git@github.com:desenvolvimentopsa/Logger.git utils/Logger
# git clone git@github.com:desenvolvimentopsa/Exchange.git utils/Exchange
# git clone git@github.com:desenvolvimentopsa/Configurator.git utils/Configurator
# cd utils
# find . -name "*.py" | xargs -I {} cp {} .
# rm -rf Logger Exchange Configurator
# cd ..

cd salesContracts
cp -r ../utils .
echo SALESCONTRACTS
docker build -t sg_bi_api_salescontracts${version} .
cd ..
rm -rf salesContracts/utils

# cd processar
# cp -r ../utils .
# echo PROCESSAR 
# docker build -t sg_bi_api_salescontracts${version} .
# cd ..
# rm -rf processar/utils
