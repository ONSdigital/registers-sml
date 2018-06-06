#!groovy
@Library('jenkins-pipeline-shared@develop') _
 
def projectName = 'registers-sml'
def distDir = 'build/dist/'
def server = Artifactory.server 'art-p-01'
def buildInfo = Artifactory.newBuildInfo()
 
pipeline {
    agent any
    // agent {node{label 'master'}}
    options {
        skipDefaultCheckout()
        buildDiscarder(logRotator(numToKeepStr: '30', artifactNumToKeepStr: '30'))
        timeout(time: 15, unit: 'MINUTES')
        timestamps()
        ansiColor('xterm')
    }
    environment {
        BUILD_ENV = "CI"
       
        ARTIFACTORY_CREDS = credentials("sbr-artifactory-user")
    }
 
    stages {
        stage('Init') {
            agent any
            steps {
                deleteDir()
                // sh 'git clean -fxd'
                checkout scm
                stash name: 'app'
                script {
                    environment {
                        STAGE_NAME = "Init"
                    }
                }
                colourText("info", "Checking out ${projectName} in stage ${STAGE_NAME}")
            }
        }
        stage('Build') {
            steps {
                script {
                    environment {
                        STAGE_NAME = "Build"
                    }
                }
                colourText("info", "Building ${env.BUILD_ID} on ${env.JENKINS_URL} from branch ${env.BRANCH_NAME}")
                sh 'mvn clean compile'
            }
        }
       
        stage('Package') {
            agent any
            steps {
                sh 'mvn package'
            }
        }
       
        stage('Static Analysis') {
            agent any
            steps {
                parallel (
                    "Java and Scala" :  {
                        colourText("info","Running Maven tests for Java wrapper and Scala")
                        sh "mvn test"
                    },
                    "Python" :  {
                        colourText("info","Using behave to run Python tests.")
                        sh """
                            pip install virtualenv -i http://${ARTIFACTORY_CREDS_USR}:${ARTIFACTORY_CREDS_PSW}@art-p-01/artifactory/api/pypi/yr-python/simple --trusted-host art-p-01 -t "$WORKSPACE/.local"
                            mkdir venv
                            python ${WORKSPACE}/.local/virtualenv.py --distribute -p /usr/bin/python2.7 ${WORKSPACE}/venv
                            source venv/bin/activate
                           
                            pip install behave -i http://${ARTIFACTORY_CREDS_USR}:${ARTIFACTORY_CREDS_PSW}@art-p-01/artifactory/api/pypi/yr-python/simple --trusted-host art-p-01
                            pip install pypandoc -i http://${ARTIFACTORY_CREDS_USR}:${ARTIFACTORY_CREDS_PSW}@art-p-01/artifactory/api/pypi/yr-python/simple --trusted-host art-p-01
                            pip install pyspark -i http://${ARTIFACTORY_CREDS_USR}:${ARTIFACTORY_CREDS_PSW}@art-p-01/artifactory/api/pypi/yr-python/simple --trusted-host art-p-01
                           
                      
                            chmod -R 777 ${WORKSPACE}/venv
                            behave --format progress2
                        """
                    },
                    "R" :  {
                        colourText("info","Using devtools to run R tests.")
                    }
                )
            }
            post {
                always {
                    script {
                        environment {
                            STAGE_NAME = "Static Analysis"
                        }
                    }
                }
                success {
                    colourText("info","Generating reports for tests")
                    junit '**/target/*-reports/*.xml'
                }
                failure {
                    colourText("warn","Failed to retrieve reports.")
                }
            }
        }
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
            // sendNotifications currentBuild.result, "\$SBR_EMAIL_LIST"
        }
        unstable {
            colourText("warn", "Something went wrong, build finished with result ${currentResult}. This may be caused by failed tests, code violation or in some cases unexpected interrupt.")
            // sendNotifications currentBuild.result, "\$SBR_EMAIL_LIST", "${STAGE_NAME}"
        }
        failure {
            colourText("warn","Process failed at: ${STAGE_NAME}")
            // sendNotifications currentBuild.result, "\$SBR_EMAIL_LIST", "${STAGE_NAME}"
        }
    }
}
 
def installPythonModule(String module){
    sh """pip install ${module} -i http://${ARTIFACTORY_CREDS_USR}:${ARTIFACTORY_CREDS_PSW}@art-p-01/artifactory/api/pypi/yr-python/simple --trusted-host art-p-01"""
}
