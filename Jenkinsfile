#!groovy
@Library('jenkins-pipeline-shared') _

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
    }
    environment {
        BUILD_ENV = "CI"
        STAGE = "Unknown"
        ARTIFACTORY_CREDS = credentials("sbr-artifactory-user")
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
                sh 'mvn clean compile'
            }
        }
        
        stage('Package') {
            agent any
            environment{
                STAGE = "Package"
            }
            steps {
                sh 'mvn package'
            }
        }
        
        stage('Static Analysis') {
            agent any
            environment{
                STAGE = "Static Analysis"
            }
            steps {
                parallel (
                    "Java and Scala" :  {
                        colourText("info","Running Maven tests for Java wrapper and Scala")
                        // sh "mvn clean test"
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
                            behave --verbose
                        """
                        // installPythonModule("behave")
                        // installPythonModule("pypandoc")
                        // installPythonModule("pyspark")
                        
                        
                    },
                    "R" :  {
                        colourText("info","Using devtools to run R tests.")
                        // sh "mvn clean test"
                    }
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
            // sendNotifications currentBuild.result, "\$SBR_EMAIL_LIST", "${STAGE}"
        }
        failure {
            colourText("warn","Process failed at: ${STAGE_NAME}")
            // sendNotifications currentBuild.result, "\$SBR_EMAIL_LIST", "${STAGE}"
        }
    }
}

def installPythonModule(String module){
    sh """pip install ${module} -i http://${ARTIFACTORY_CREDS_USR}:${ARTIFACTORY_CREDS_PSW}@art-p-01/artifactory/api/pypi/yr-python/simple --trusted-host art-p-01"""
}

