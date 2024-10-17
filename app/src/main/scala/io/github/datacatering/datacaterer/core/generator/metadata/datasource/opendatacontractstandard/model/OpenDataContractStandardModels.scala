package io.github.datacatering.datacaterer.core.generator.metadata.datasource.opendatacontractstandard.model

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

@JsonIgnoreProperties(ignoreUnknown = true)
case class OpenDataContractStandard(
                                     dataset: Array[OpenDataContractStandardDataset],
                                     datasetName: String,
                                     kind: String,
                                     quantumName: String,
                                     status: String,
                                     uuid: String,
                                     version: String,
                                     apiVersion: Option[String] = None,
                                     database: Option[String] = None,
                                     datasetDomain: Option[String] = None,
                                     datasetKind: Option[String] = None,
                                     description: Option[OpenDataContractStandardDescription] = None,
                                     driver: Option[String] = None,
                                     driverVersion: Option[String] = None,
                                     password: Option[String] = None,
                                     price: Option[OpenDataContractStandardPrice] = None,
                                     productDl: Option[String] = None,
                                     productFeedbackUrl: Option[String] = None,
                                     productSlackChannel: Option[String] = None,
                                     project: Option[String] = None,
                                     roles: Option[Array[OpenDataContractStandardRole]] = None,
                                     tags: Option[Array[String]] = None,
                                     tenant: Option[String] = None,
                                     `type`: Option[String] = None,
                                     server: Option[String] = None,
                                     slaDefaultColumn: Option[String] = None,
                                     slaProperties: Option[Array[OpenDataContractStandardServiceLevelAgreementProperty]] = None,
                                     sourceSystem: Option[String] = None,
                                     sourcePlatform: Option[String] = None,
                                     stakeholders: Option[Array[OpenDataContractStandardStakeholder]] = None,
                                     username: Option[String] = None,
                                     userConsumptionMode: Option[String] = None,
                                   )

@JsonIgnoreProperties(ignoreUnknown = true)
case class OpenDataContractStandardDescription(
                                                limitations: Option[String] = None,
                                                purpose: Option[String] = None,
                                                usage: Option[String] = None
                                              )

@JsonIgnoreProperties(ignoreUnknown = true)
case class OpenDataContractStandardDataset(
                                            table: String,
                                            authoritativeDefinitions: Option[Array[OpenDataContractStandardAuthoritativeDefinition]] = None,
                                            columns: Option[Array[OpenDataContractStandardColumn]] = None,
                                            dataGranularity: Option[String] = None,
                                            description: Option[String] = None,
                                            physicalName: Option[String] = None,
                                            priorTableName: Option[String] = None,
                                            quality: Option[Array[OpenDataContractStandardDataQuality]] = None,
                                          )

@JsonIgnoreProperties(ignoreUnknown = true)
case class OpenDataContractStandardColumn(
                                           column: String,
                                           logicalType: String,
                                           physicalType: String,
                                           authoritativeDefinitions: Option[Array[OpenDataContractStandardAuthoritativeDefinition]] = None,
                                           businessName: Option[String] = None,
                                           criticalDataElementStatus: Option[Boolean] = None,
                                           classification: Option[String] = None,
                                           clusterKeyPosition: Option[Int] = None,
                                           clusterStatus: Option[Boolean] = None,
                                           description: Option[String] = None,
                                           encryptedColumnName: Option[String] = None,
                                           isNullable: Option[Boolean] = None,
                                           isPrimaryKey: Option[Boolean] = None,
                                           isUnique: Option[Boolean] = None,
                                           partitionKeyPosition: Option[Int] = None,
                                           partitionStatus: Option[Boolean] = None,
                                           primaryKeyPosition: Option[Int] = None,
                                           sampleValues: Option[Array[String]] = None,
                                           tags: Option[Array[String]] = None,
                                           transformDescription: Option[String] = None,
                                           transformLogic: Option[String] = None,
                                           transformSourceTables: Option[Array[String]] = None,
                                         )

@JsonIgnoreProperties(ignoreUnknown = true)
case class OpenDataContractStandardDataQuality(
                                                templateName: String,
                                                toolName: String,
                                                businessImpact: Option[String] = None,
                                                code: Option[String] = None,
                                                column: Option[String] = None,
                                                columns: Option[String] = None,
                                                customProperties: Option[Array[OpenDataContractStandardCustomProperty]] = None,
                                                description: Option[String] = None,
                                                dimension: Option[String] = None,
                                                scheduleCronExpression: Option[String] = None,
                                                severity: Option[String] = None,
                                                toolRuleName: Option[String] = None,
                                                `type`: Option[String] = None,
                                              )

@JsonIgnoreProperties(ignoreUnknown = true)
case class OpenDataContractStandardPrice(
                                          priceAmount: Option[Double] = None,
                                          priceCurrency: Option[String] = None,
                                          priceUnit: Option[String] = None,
                                        )

@JsonIgnoreProperties(ignoreUnknown = true)
case class OpenDataContractStandardStakeholder(
                                                dateIn: Option[String] = None,
                                                dateOut: Option[String] = None,
                                                username: Option[String] = None,
                                                replacedByUsername: Option[String] = None,
                                                role: Option[String] = None,
                                              )

@JsonIgnoreProperties(ignoreUnknown = true)
case class OpenDataContractStandardRole(
                                         access: String,
                                         role: String,
                                         firstLevelApprovers: Option[String] = None,
                                         secondLevelApprovers: Option[String] = None,
                                       )

@JsonIgnoreProperties(ignoreUnknown = true)
case class OpenDataContractStandardServiceLevelAgreementProperty(
                                                                  property: String,
                                                                  value: String,
                                                                  column: Option[String] = None,
                                                                  driver: Option[String] = None,
                                                                  unit: Option[String] = None,
                                                                  valueExt: Option[String] = None,
                                                                )

@JsonIgnoreProperties(ignoreUnknown = true)
case class OpenDataContractStandardAuthoritativeDefinition(
                                                            `type`: String,
                                                            url: String,
                                                          )

@JsonIgnoreProperties(ignoreUnknown = true)
case class OpenDataContractStandardCustomProperty(
                                                   property: String,
                                                   value: String,
                                                 )