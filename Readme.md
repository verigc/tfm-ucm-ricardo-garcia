# TFM: Pipeline de Datos Serverless en AWS
Este repositorio contiene el código fuente y la plantilla de infraestructura para el Trabajo Fin de Máster, que consiste en un pipeline de datos automatizado para la ingesta y procesamiento de fuentes públicas, construido con tecnologías serverless de AWS.

El proyecto se despliega utilizando el framework AWS SAM (Serverless Application Model), lo que garantiza que toda la arquitectura pueda ser replicada de forma consistente y automática.

Arquitectura
La solución se compone de los siguientes recursos principales de AWS:

AWS Step Functions: Orquesta el flujo de trabajo completo, desde la ingesta hasta la transformación final.

AWS Lambda: Ejecuta la lógica de extracción de datos de las diferentes fuentes públicas en formato Python.

AWS Glue: Realiza el trabajo de ETL final para transformar y particionar los datos en el Data Lake.

Amazon S3: Actúa como Data Lake, almacenando los datos en diferentes etapas (crudo y procesado).

IAM Roles: Define los permisos necesarios para que los servicios interactúen entre sí de forma segura.

Prerrequisitos
Antes de comenzar, asegúrate de tener instaladas y configuradas las siguientes herramientas en tu máquina local:

Una cuenta de AWS: con permisos para crear los recursos mencionados.

AWS CLI: [Instrucciones de instalación](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html). Debes tenerla configurada con tus credenciales (aws configure).

AWS SAM CLI: [Instrucciones de instalación](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html).

Git: Para clonar este [repositorio](https://github.com).

Instrucciones de Despliegue
Para desplegar la arquitectura completa en tu cuenta de AWS, sigue estos pasos desde tu terminal:

1. Clonar el Repositorio

Bash

git clone [https://github.com/verigc/tfm-ucm-ricardo-garcia.git]
cd [tfm-ucm-ricardo-garcia]
2. Construir el Proyecto
Este comando preparará los artefactos de la aplicación, instalará las dependencias de cada función Lambda y las dejará listas para ser empaquetadas.

Bash

sam build
3. Desplegar la Aplicación
Este es el comando principal que creará todos los recursos en la nube. Usaremos el modo guiado (--guided) para que el proceso sea más sencillo. La primera vez que lo ejecutes, te hará una serie de preguntas para configurar el despliegue.

Bash

sam deploy --guided
A continuación, se te pedirá que introduzcas los siguientes parámetros. Puedes aceptar los valores por defecto presionando Enter o especificarlos:

Stack Name: Un nombre para tu proyecto en AWS (ej: tfm-data-pipeline).

AWS Region: La región donde quieres desplegar (ej: us-east-1).

Confirm changes before deploy: Responde y (sí) para poder revisar los cambios.

Allow SAM CLI IAM role creation: Responde y para permitir que SAM cree los roles de permisos necesarios.

Save arguments to samconfig.toml: Responde y para guardar tus respuestas. Así, las próximas veces que ejecutes sam deploy, no tendrás que volver a introducirlas.

El proceso de despliegue tardará unos minutos. Una vez finalizado, todos los recursos estarán creados y activos en tu cuenta de AWS.

Validación
Para comprobar que el despliegue ha sido exitoso:

Inicia sesión en la consola de AWS.

Navega al servicio Step Functions.

Deberías ver una nueva máquina de estados llamada MiPipelinePrincipal (o el nombre que le hayas dado).

Puedes iniciar una ejecución manual para probar el pipeline de extremo a extremo.

Limpieza (Destrucción de Recursos)
Para eliminar todos los recursos creados por este proyecto y evitar costes inesperados, puedes eliminar el "stack" completo ejecutando el siguiente comando:

Bash

# Reemplaza 'tfm-data-pipeline' con el nombre que le diste al Stack Name
sam delete --stack-name tfm-data-pipeline