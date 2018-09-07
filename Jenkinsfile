#!groovy
@Library('jenkins-pipeline-shared') _

def projectName = 'registers-sml'
def distDir = 'build/dist/'
def server = Artifactory.server 'art-p-01'
// def buildInfo = Artifactory.newBuildInfo()
def rtMaven = Artifactory.newMavenBuild()
rtMaven.deployer server: server, releaseRepo: 'LR_Registers-Releases', snapshotRepo: 'LR_Registers-Snapshots'


rtMaven.resolver server: server, releaseRepo: 'ons-repo', snapshotRepo: 'ons-repo'

rtMaven.tool = 'Maven-3.3.9'

pipeline {
    agent any
    // agent {node{label 'master'}}
    options {
        skipDefaultCheckout()
        buildDiscarder(logRotator(numToKeepStr: '30', artifactNumToKeepStr: '30'))
        timeout(time: 15, unit: 'MINUTES')
        timestamps()
    }
    environment {
        BUILD_ENV = "CI"
        STAGE = "Unknown"
        ARTIFACTORY_CREDS = credentials("sbr-artifactory-user")
        ARTIFACTORY_HOST_NAME = "art-p-01"
        MASTER = "master"
        SBR_METHODS_ARTIFACTORY_PYPI_REPO = "sbr-methods-pypi"

        ENV = "${params.ENV_NAME}"
        PROD1_NODE = "cdhdn-p01-01"
        SSH_KEYNAME = "sbr-${params.ENV}-ci-ssh-key"
        MODULE_NAME = 'registers-sml'
    }
    parameters {
        choice(choices: 'dev\ntest\nbeta', description: 'Into what environment wants to deploy oozie config e.g. dev, test or beta?', name: 'ENV_NAME')
    }
    stages {
        stage('Init') {
            agent any
            environment{
                STAGE = "Init"
            }
            steps {
                deleteDir()
                // sh 'git clean -fxd'
                checkout scm
                stash name: 'app'
                colourText("info", "Checking out ${projectName} in stage ${STAGE_NAME}")
            }
        }
        stage('Build') {
            environment{
                STAGE = "Build"
            }
            steps {
                colourText("info", "Building ${env.BUILD_ID} on ${env.JENKINS_URL} from branch ${env.BRANCH_NAME}")
                readFile('/usr/share/maven/conf/settings.xml')
            }
        }
        stage('Static Analysis') {
            agent any
            environment{
                STAGE = "Static Analysis"
            }
            steps {
                parallel (
                        "Java and Scala": {
                            colourText("info","Running Maven tests for Java wrapper and Scala")
                            sh "mvn clean test"
                        }
                        // "Python": {
                        //     colourText("info","Using behave to run Python tests.")
                        //     sh """
                        //         pip install virtualenv -i http://${ARTIFACTORY_CREDS_USR}:${ARTIFACTORY_CREDS_PSW}@${ARTIFACTORY_HOST_NAME}/artifactory/api/pypi/yr-python/simple --trusted-host ${ARTIFACTORY_HOST_NAME} -t "$WORKSPACE/.local"
                        //         mkdir venv
                        //         python ${WORKSPACE}/.local/virtualenv.py --distribute -p /usr/bin/python2.7 ${WORKSPACE}/venv
                        //         source venv/bin/activate

                        //         pip install behave -i http://${ARTIFACTORY_CREDS_USR}:${ARTIFACTORY_CREDS_PSW}@${ARTIFACTORY_HOST_NAME}/artifactory/api/pypi/yr-python/simple --trusted-host ${ARTIFACTORY_HOST_NAME}
                        //         pip install pypandoc -i http://${ARTIFACTORY_CREDS_USR}:${ARTIFACTORY_CREDS_PSW}@${ARTIFACTORY_HOST_NAME}/artifactory/api/pypi/yr-python/simple --trusted-host ${ARTIFACTORY_HOST_NAME}
                        //         pip install pyspark -i http://${ARTIFACTORY_CREDS_USR}:${ARTIFACTORY_CREDS_PSW}@${ARTIFACTORY_HOST_NAME}/artifactory/api/pypi/yr-python/simple --trusted-host ${ARTIFACTORY_HOST_NAME}

                        //         chmod -R 777 ${WORKSPACE}/venv
                        //         behave --verbose
                        //     """
                        //     // installPythonModule("behave")
                        //     // installPythonModule("pypandoc")
                        //     // installPythonModule("pyspark")
                        // },
                        // "R": {
                        //     colourText("info","Using devtools to run R tests.")
                        //     // sh "devtools::test_dir($WORKSPACE/R)"
                        // }
                )
            }
            post {
                success {
                    colourText("info","Generating reports for tests")
                    // junit '**/target/*-reports/*.xml'
                }
                failure {
                    colourText("warn","Failed to retrieve reports.")
                }
            }
        }
        stage('Package') {
            agent any
            environment{
                STAGE = "Package"
            }
            steps {
                script {
                    def buildInfo = rtMaven.run pom: '${WORKSPACE}/pom.xml', goals: 'clean install'      
                }
            }
        }
        stage("Push to Artifactory") {
            agent any
            environment{
                STAGE = "Push to Artifactory"
            }
            steps {
                script {
                    Artifactory.newMavenBuild()
                }
            }
        }

        // stage('PyPackage'){
        //     agent any
        //     when {
        //         branch MASTER
        //     }
        //     steps {
        //         script {
        //             colourText("info","Python Package Stage")
        //             colourText("info", "Update Versions before packaging")
        //             sh 'cd python && python setup.py sdist'
        //         }
        //     }
        // }
        // stage ('PyDeploy '){
        //     agent any
        //     when {
        //         branch MASTER
        //     }
        //     steps {
        //         script {
        //             println('Final Deploy Stage')
        //             sh """
        //                 curl -u ${ARTIFACTORY_CREDS_USR}:${ARTIFACTORY_CREDS_PSW} -T python/dist/* "http://${ARTIFACTORY_HOST_NAME}/artifactory/${SBR_METHODS_ARTIFACTORY_PYPI_REPO}/"
        //             """
        //         }
        //     }
        //     post {
        //         success {
        //             colourText("success", "Successfully push to pypi art repository!")
        //         }
        //         failure {
        //             colourText("warn","Failed to archive")
        //         }
        //     }
        // }
    }
    post {
        always {
            script {
                colourText("info", 'Post steps initiated')
                deleteDir()
            }
        }
        success {
            colourText("success", "All stages complete. Build was successful.")
            sendNotifications currentBuild.result, "\$SBR_EMAIL_LIST"
        }
        unstable {
            colourText("warn", "Something went wrong, build finished with result ${currentBuild.result}. This may be caused by failed tests, code violation or in some cases unexpected interrupt.")
            sendNotifications currentBuild.result, "\$SBR_EMAIL_LIST", "${STAGE}"
        }
        failure {
            colourText("warn","Process failed at: ${STAGE}")
            sendNotifications currentBuild.result, "\$SBR_EMAIL_LIST", "${STAGE}"
        }
    }
}

def installPythonModule(String module){
    sh """pip install ${module} -i http://${ARTIFACTORY_CREDS_USR}:${ARTIFACTORY_CREDS_PSW}@${ARTIFACTORY_HOST_NAME}/artifactory/api/pypi/yr-python/simple --trusted-host ${ARTIFACTORY_HOST_NAME}"""
}

def archiveToHDFS(){
    echo "archiving jar to [$ENV] environment"
    sshagent(credentials: ["sbr-$ENV-ci-ssh-key"]){
        sh """
            ssh sbr-$ENV-ci@$PROD1_NODE mkdir -p $MODULE_NAME/
            echo "Successfully created new directory [$MODULE_NAME/]"
            scp -r $WORKSPACE/$MODULE_NAME/* sbr-$ENV-ci@$PROD1_NODE:$MODULE_NAME/
            echo "Successfully moved artifact [JAR] and scripts to $MODULE_NAME/"
            ssh sbr-$ENV-ci@$PROD1_NODE hdfs dfs -put -f $MODULE_NAME/ hdfs://prod1/user/sbr-$ENV-ci/lib/
            echo "Successfully copied jar file to HDFS"

        """
    }
}
