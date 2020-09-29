DockerImagePython = 'neuron-bif-python36-airflow:v1.0.2'
PytestJUnitFilePath = 'tests/target/junit/coverage.xml'
PytestCovFilePath = 'tests/target/pytest-cov/coverage.xml'

pipeline {
    agent {
        kubernetes {
            containerTemplate {
                name 'python3-container'
                image "eu.gcr.io/${SS_PROJECT}/${DockerImagePython}"
                ttyEnabled true
                command 'cat'
                workingDir '/home/jenkins/agent'
                resourceRequestCpu '0.5'
                resourceLimitCpu '2'
                resourceRequestMemory '2Gi'
                resourceLimitMemory '16Gi'
            }
        }
    }

    stages {
        stage('Clone repository') {
            steps {
                checkout scm
            }
        }

        stage('Install Python packages') {
            steps {
                container("python3-container") {
                   sh script: '''
                        pip install --proxy outbound-proxy.neuron.shared-services.vf.local:3128 -r requirements.txt
                   '''
                }
            }
        }

        stage('Pycodestyle check') {
            steps {
                container("python3-container") {
                    sh 'pycodestyle *'
                }
            }
        }

        stage('Pytest') {
            steps {
                container("python3-container") {
                    sh script: """
                        python -m pytest tests \
                            --junitxml=$PytestJUnitFilePath \
                            --durations 10 \
                            --cov . \
                            --cov-config .coveragerc \
                            --cov-report xml:$PytestCovFilePath

                        # Convert PyTest xUnit test reports to valid xUnit format
                        # https://issues.jenkins-ci.org/browse/JENKINS-51920
                        xsltproc pytest-xunit.xsl $PytestJUnitFilePath > ${PytestJUnitFilePath}.tmp
                        rm -f $PytestJUnitFilePath
                        mv ${PytestJUnitFilePath}.tmp $PytestJUnitFilePath
                    """
                }
            }
        }
    }

    post {
        always {
            xunit thresholds: [failed(failureNewThreshold: '0', failureThreshold: '0', unstableNewThreshold: '0', unstableThreshold: '0')], tools: [JUnit(
                    deleteOutputFiles: true,
                    failIfNotNew: false,
                    pattern: '**/' + PytestJUnitFilePath,
                    skipNoTestFiles: false,
                    stopProcessingIfError: false)]

            cobertura autoUpdateHealth: false,
                    autoUpdateStability: false,
                    classCoverageTargets: '80, 0, 80',
                    coberturaReportFile: '**/' + PytestCovFilePath,
                    conditionalCoverageTargets: '80, 0, 80',
                    lineCoverageTargets: '85, 0, 85',
                    maxNumberOfBuilds: 0,
                    methodCoverageTargets: '80, 0, 80',
                    onlyStable: false,
                    failUnhealthy: true,
                    failUnstable: true,
                    sourceEncoding: 'ASCII',
                    zoomCoverageChart: false
        }
    }
}