/*============================================================================*/
/* AdaptorWL.cpp -- Wire Line Adaptor                                         */
/*============================================================================*/

#include "BASE.h"

#include "thrift/protocol/TCompactProtocol.h"
#include "thrift/transport/TBufferTransports.h"

#include "AdaptorWL.hpp"
#include "BufferTransport.hpp"
#include "WlpHeader.hpp"
#include "Version.h"
#include "StatementContext.hpp"
#include "IndexDef.hpp"
#include "TailLine.hpp"
#include "RowInflator.hpp"
#include "SP_Bcd.hpp"
#include "Job.hpp"
#include "TagConstants.h"

// All of the wireline protocol messages
#include "wlp/thrift/generated/AuthenticationOkResponse_types.h"
#include "wlp/thrift/generated/ClearTextTunnelRequest_types.h"
#include "wlp/thrift/generated/ClosePreparedStatementRequest_types.h"
#include "wlp/thrift/generated/ClosePreparedStatementResponse_types.h"
#include "wlp/thrift/generated/ColumnMetadata_types.h"
#include "wlp/thrift/generated/ColumnValue_types.h"
#include "wlp/thrift/generated/DisconnectNotification_types.h"
#include "wlp/thrift/generated/DisconnectRequest_types.h"
#include "wlp/thrift/generated/ErrorResponse_types.h"
#include "wlp/thrift/generated/GssNegotiationMsg_types.h"
#include "wlp/thrift/generated/KerberosAuthenticationRequest_types.h"
#include "wlp/thrift/generated/MessageTypes_types.h"
#include "wlp/thrift/generated/ParseStatementRequest_types.h"
#include "wlp/thrift/generated/ParseStatementResponse_types.h"
#include "wlp/thrift/generated/PingRequest_types.h"
#include "wlp/thrift/generated/PingResponse_types.h"
#include "wlp/thrift/generated/PlainAuthenticationRequest_types.h"
#include "wlp/thrift/generated/ProtocolCompatibleRequest_types.h"
#include "wlp/thrift/generated/ProtocolCompatibleResponse_types.h"
#include "wlp/thrift/generated/ProtocolIncompatibleResponse_types.h"
#include "wlp/thrift/generated/QueryExecuteRequest_types.h"
#include "wlp/thrift/generated/QueryExecuteResponse_types.h"
#include "wlp/thrift/generated/QueryProgressRequest_types.h"
#include "wlp/thrift/generated/QueryProgressResponse_types.h"
#include "wlp/thrift/generated/QueryReceiptResponse_types.h"
#include "wlp/thrift/generated/RowDataResponse_types.h"
#include "wlp/thrift/generated/RowMetadataResponse_types.h"
#include "wlp/thrift/generated/SessionChangeRequest_types.h"
#include "wlp/thrift/generated/SessionChangeResponse_types.h"
#include "wlp/thrift/generated/SessionDataResponse_types.h"
#include "wlp/thrift/generated/SessionDroppedMessagesResponse_types.h"
#include "wlp/thrift/generated/SessionMonitoringRequest_types.h"
#include "wlp/thrift/generated/SessionMonitoringResponse_types.h"
#include "wlp/thrift/generated/SessionProtocolVersionResponse_types.h"
#include "wlp/thrift/generated/ShutdownNotification_types.h"
#include "wlp/thrift/generated/StatementBindAndExecuteRequest_types.h"
#include "wlp/thrift/generated/StatementResponse_types.h"
#include "wlp/thrift/generated/TunnelSupportedResponse_types.h"
#include "wlp/thrift/generated/WarningResponse_types.h"

const i8 C_MIN_SUPPORT_PROTOCOL_VER = 5;
const i8 C_MAX_SUPPORT_PROTOCOL_VER = 5;

// If the RowDataResponse message is too big to fit into the payload then we scale
// the number of rows in it by this amount until it does fit.
const d8 C_ROWDATARESP_SCALE_DOWN = 0.8;  // Multiplied
const d8 C_ROWDATARESP_SCALE_UP   = 0.05; // Added

using namespace com::rapidsdata::wirelineprotocol::messages::compatibility;
using namespace com::rapidsdata::wirelineprotocol::messages::v5;

Stz* AdaptorWL::aState[eState::NENUMS] {
    /* 00 NONE */                     "NONE"_Stz,
    /* 01 CONNECTED */                "CONNECTED"_Stz,
    /* 02 PROTOCOL_COMPATIBLE */      "PROTOCOL_COMPATIBLE"_Stz,
    /* 03 TUNNELED */                 "TUNNELED"_Stz,
    /* 04 KERBEROS_NEGOTIATING */     "KERBEROS_NEGOTIATING"_Stz,
    /* 05 AUTHENTICATED */            "AUTHENTICATED"_Stz,
    /* 06 READY */                    "READY"_Stz,
    /* 07 QUERY_BEGUN */              "QUERY_BEGUN"_Stz,
    /* 08 API_STATEMENT_TO_SUBMIT */  "API_STATEMENT_TO_SUBMIT"_Stz,
    /* 09 API_STATEMENT_SUBMITTED */  "API_STATEMENT_SUBMITTED"_Stz,
    /* 10 API_METADATA_RECEIVED */    "API_METADATA_RECEIVED"_Stz,
    /* 11 API_DATA_RECEIVED */        "API_DATA_RECEIVED"_Stz,
    /* 12 API_STATEMENT_COMPLETED */ "API_STATEMENT_COMPLETED"_Stz,
    /* 13 API_CHILD_JOB_EXITED */    "API_CHILD_JOB_EXITED"_Stz,
    /* 14 QUERY_COMPLETED */         "QUERY_COMPLETED"_Stz,
    /* 15 DISCONNECTED */            "DISCONNECTED"_Stz,
//  /* 16 NENUMS */                  "NENUM"_Stz
};

// Static function definitions
static com::rapidsdata::wirelineprotocol::messages::v5::AffectedVerb::type
  eToWLAffectedVerb(WLP::StatementContext::eAffectedVerb eVerb);



/*----------------------------------------------------------------------------*/
/*-- LifeCycle ---------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

__pCastFunction(STORE, AdaptorWL, CN_ADAPTORWL)
__aNewFunction(AdaptorWL, CN_ADAPTORWL)
__vDeleteFunction(AdaptorWL, CN_ADAPTORWL)
__aFooterFunction(AdaptorWL)


/*----------------------------------------------------------------------------*/
/* constructor                                                                */
/*----------------------------------------------------------------------------*/
AdaptorWL::AdaptorWL(Stz* szClientAddr)
: Object(CN_ADAPTORWL)
{
  _precondition_(_ADDRESS_, _MAGIC_, CN_ADAPTORWL, _FOOTER_);
  ASSERT(szClientAddr);

  m_sClientAddr.vCopy(szClientAddr);
  m_pCurrentMsg = nullptr;
  m_iCurrentMsgType = -1;
  m_pPayloadSendList = new PayloadList();
  m_eProtocolError = EC_NONE;
  m_eProtocolState = CONNECTED;
  m_bGotGracefulDisconnect = false;
  m_uPayloadCreationTime = 0;

  //m_oQueryCtx
  m_pStmtCtx = nullptr;

  m_pTransport = std::make_shared<WLP::BufferTransport>();
  m_pProtocol  = std::make_shared<apache::thrift::protocol::TCompactProtocol>(m_pTransport);

  _checkInitialised_(CN_ADAPTORWL);

  _postcondition_(_ADDRESS_, _MAGIC_, CN_ADAPTORWL, _FOOTER_);
}

/*----------------------------------------------------------------------------*/
/* destructor                                                                 */
/*----------------------------------------------------------------------------*/
AdaptorWL::~AdaptorWL(void) noexcept(false)
{
  _precondition_(_ADDRESS_, _MAGIC_, CN_ADAPTORWL, _FOOTER_);
  if (m_pCurrentMsg) {
    delete m_pCurrentMsg;
    m_pCurrentMsg = nullptr;
  }
  m_pPayloadSendList->vDeleteAll();
  delete m_pPayloadSendList;

  delete m_pStmtCtx;

  // m_oQueryCtx  will be cleaned up automatically
  // m_pTransport will be cleaned up automatically
  // m_pProtocol  will be cleaned up automatically
}

/*----------------------------------------------------------------------------*/
/* repair the aggregate time                                                  */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vRepair(Object* pObject)
{
  _static_

  AdaptorWL* pAdaptorWL = _pAdaptorWL(pObject);
  delete pAdaptorWL;
}

/*----------------------------------------------------------------------------*/
/*-- Public ------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
/* Get the current client's IP and port                                       */
/*----------------------------------------------------------------------------*/
Stz* AdaptorWL::szClientAddr()
{
  return m_sClientAddr.szStz();
}

/*----------------------------------------------------------------------------*/
/* Decode the payload into a wireline message                                 */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vDecodePayload(WLP::PayloadObj* pPayload)
{
  _precondition_(_ADDRESS_,_MAGIC_,CN_ADAPTORWL,_FOOTER_);
  ASSERT(m_eProtocolError == EC_NONE);
  ASSERT(!m_pCurrentMsg);
  ASSERT(m_iCurrentMsgType == -1);

  m_uPayloadCreationTime = pPayload->uCreationTime();

  ASSERT(pPayload->iPayloadSize() > WLP::C_WLPHEADER_SIZE);
  WLP::WlpHeader* pHeader = (WLP::WlpHeader*) pPayload->pcBuf();

  ASSERT(pHeader->iMsgSize() > WLP::C_WLPHEADER_SIZE);
  ASSERT(pHeader->iMsgType() >= 0);

  i8 iMsgBodySize = pHeader->iMsgSize() - WLP::C_WLPHEADER_SIZE;
  MessageTypes::type eMsgType = (MessageTypes::type) pHeader->iMsgType();

  m_pTransport->vUseBuffer(pHeader->pcMsgBody(), iMsgBodySize);
  vDeserialiseMsg(eMsgType);
  m_pTransport->vRemoveBuffer();
}

/*----------------------------------------------------------------------------*/
/* Process or continue processing the current decoded wireline message        */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vProcessWLMsg(void)
{
  _precondition_(_ADDRESS_,_MAGIC_,CN_ADAPTORWL,_FOOTER_);

  ASSERT(m_iCurrentMsgType != -1);
  ASSERT(m_pCurrentMsg);
  ASSERT(m_eProtocolError == EC_NONE);

  MessageTypes::type eMsgType = (MessageTypes::type) m_iCurrentMsgType;

  switch (eMsgType) {

    case MessageTypes::type::ProtocolCompatibleRequest:       vProcessProtocolCompatibleRequest();      break;
    case MessageTypes::type::DisconnectRequest:               vProcessDisconnectRequest();              break;
    case MessageTypes::type::PingRequest:                     vProcessPingRequest();                    break;
    case MessageTypes::type::ClearTextTunnelRequest:          vProcessClearTextTunnelRequest();         break;
    case MessageTypes::type::PlainAuthenticationRequest:      vProcessPlainAuthenticationRequest();     break;
    case MessageTypes::type::KerberosAuthenticationRequest:   vProcessKerberosAuthenticationRequest();  break;
    case MessageTypes::type::GssNegotiationMsg:               vProcessGssNegotiationMsg();              break;
    case MessageTypes::type::SessionChangeRequest:            vProcessSessionChangeRequest();           break;
    case MessageTypes::type::QueryExecuteRequest:             vProcessQueryExecuteRequest();            break;
    case MessageTypes::type::QueryProgressRequest:            vProcessQueryProgressRequest();           break;
    case MessageTypes::type::SessionMonitoringRequest:        vProcessSessionMonitoringRequest();       break;
    case MessageTypes::type::ParseStatementRequest:           vProcessParseStatementRequest();          break;
    case MessageTypes::type::StatementBindAndExecuteRequest:  vProcessStatementBindAndExecuteRequest(); break;
    case MessageTypes::type::ClosePreparedStatementRequest:   vProcessClosePreparedStatementRequest();  break;

      // The following are messages that should never be received by RapidsSE
      // as they should never be sent by a client. Only sent by a server.
      //   ProtocolCompatibleResponse:
      //   ProtocolIncompatibleResponse:
      //   ErrorResponse:
      //   WarningResponse:
      //   ShutdownNotification:
      //   DisconnectNotification:
      //   PingResponse:
      //   TunnelSupportedResponse:
      //   AuthenticationOkResponse:
      //   SessionChangeResponse:
      //   QueryReceiptResponse:
      //   RowMetadataResponse:
      //   RowDataResponse:
      //   StatementResponse:
      //   QueryExecuteResponse:
      //   QueryProgressResponse:
      //   SessionProtocolVersionResponse:
      //   SessionDataResponse:
      //   SessionDroppedMessagesResponse:
      //   SessionMonitoringResponse:
      //   ParseStatementResponse:
      //   ClosePreparedStatementResponse:

    default:
      // Respond to the client with an error, and set the m_eProtocolError
      // which will cause the socket to be closed.
      std::string sMsgType = to_string(eMsgType);
      eEC eErrCode = EC_WL_PROTOCOL_ERROR;
      vSerialiseErrorResponse(eErrCode,
                              "Error: %s; %s; Processing an unexpected wireline message type of %s."_Stz,
                              aEC2[eErrCode],
                              aErrorMsg[eErrCode],
                              sMsgType.c_str());

      vDisconnectClient(EC_WL_PROTOCOL_ERROR);
      return;
  }


  // Delete m_pCurrentMsg when processing is all done
  vCleanupCurrentMsg();
}

/*----------------------------------------------------------------------------*/
/* Return true if a protocol error was hit and the socket should be closed    */
/*----------------------------------------------------------------------------*/
eEC AdaptorWL::eProtocolError(void)
{
  _precondition_(_ADDRESS_,_MAGIC_,CN_ADAPTORWL,_FOOTER_);

  return m_eProtocolError;
}

/*----------------------------------------------------------------------------*/
/* Return true if there is at least 1 PayloadObj to be sent                   */
/*----------------------------------------------------------------------------*/
bool AdaptorWL::bHasPayloadToSend(void)
{
  _precondition_(_ADDRESS_,_MAGIC_,CN_ADAPTORWL,_FOOTER_);

  return (m_pPayloadSendList->nItems() > 0);
}

/*----------------------------------------------------------------------------*/
/* Return the next PayloadObj to be sent                                      */
/*----------------------------------------------------------------------------*/
WLP::PayloadObj* AdaptorWL::pNextPayloadToSend(void)
{
  _precondition_(_ADDRESS_,_MAGIC_,CN_ADAPTORWL,_FOOTER_);
  return m_pPayloadSendList->pPop();
}

/*----------------------------------------------------------------------------*/
/* Is there a query to be parsed through the SE API                           */
/*----------------------------------------------------------------------------*/
bool AdaptorWL::bHasQuery()
{
  return (m_oQueryCtx.szQueryText() != nullptr);
}

/*----------------------------------------------------------------------------*/
/* Get the query string to be parsed through the SE API.                      */
/* May contain multiple statements.                                           */
/*----------------------------------------------------------------------------*/
Stz* AdaptorWL::szQuery()
{
  Stz* szQuery = m_oQueryCtx.szQueryText();
  ASSERT(szQuery);
  return szQuery;
}

/*----------------------------------------------------------------------------*/
/* Do we need to abort any remaining statements?                              */
/*----------------------------------------------------------------------------*/
bool AdaptorWL::bAbortEarly()
{
  return m_oQueryCtx.bAbortEarly();
}

/*----------------------------------------------------------------------------*/
/* Return the time when the last payload was created                          */
/*----------------------------------------------------------------------------*/
u8 AdaptorWL::uPayloadCreationTime()
{
  return m_uPayloadCreationTime;
}

/*----------------------------------------------------------------------------*/
/* Update the state that a statement has been submitted                       */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vStatementToSubmit(Task* pNextTask)
{
  if (!bCheckState( {QUERY_BEGUN, API_CHILD_JOB_EXITED} )) {
    return;
  }

  vNextState(API_STATEMENT_TO_SUBMIT);

  // Create a statement context to hold information about the running of
  // this current statement in the (potential) batch.

  ASSERT(!m_pStmtCtx);

  Stz* szCommand = pNextTask->pJob()->pJobTags()->szValue(C_TAG_STMT);
  ASSERT(szCommand);

  s256 sJobId;
  sJobId.vSprint("%lld"_Stz, pNextTask->pJob()->tJobId().iAsI8());

  m_pStmtCtx = new WLP::StatementContext(szCommand,
                                         sJobId.szStz());

  m_oQueryCtx.vIncStmtParsed();
}

/*----------------------------------------------------------------------------*/
/* Update the state that a statement has been submitted                       */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vStatementSubmitted()
{
  ASSERT(m_pStmtCtx);
  if (!bCheckState( {API_STATEMENT_TO_SUBMIT} )) {
    return;
  }

  vNextState(API_STATEMENT_SUBMITTED);
}

/*----------------------------------------------------------------------------*/
/* Update the state that a statement has failed to submit                     */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vStatementParsingFailed(eEC eErrCode, Stz* szErrMsg)
{
  if (!bCheckState( {QUERY_BEGUN} )) {
    return;
  }

  Stz* szErrFmt= "Error: %s; %s"_Stz;

  vSerialiseErrorResponse(eErrCode, szErrFmt, aEC2[eErrCode], szErrMsg);
  ASSERT(!m_pStmtCtx);
}

/*----------------------------------------------------------------------------*/
/* Update the state that a statement has failed to submit                     */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vStatementSubmissionFailed(eEC eErrCode)
{
  if (!bCheckState( {API_STATEMENT_TO_SUBMIT, API_STATEMENT_SUBMITTED} )) {
    return;
  }

  Stz* szErrMsg= "Error: %s; %s"_Stz;

  vSerialiseErrorResponse(eErrCode, szErrMsg, aEC2[eErrCode], aErrorMsg[eErrCode]);
  m_pStmtCtx->vGotError(true);

  // The child job failed during submission.
  vChildJobExited();
}

/*----------------------------------------------------------------------------*/
/* Update the state after an error is received from the SQL compiler          */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vCompilerError(Pds* pPds)
{
  ASSERT(m_pStmtCtx);

  if (m_pStmtCtx->bGotError()) {
    return;
  }

  if (!bCheckState( {API_STATEMENT_SUBMITTED, API_METADATA_RECEIVED, API_DATA_RECEIVED} )) {
    return;
  }

  ASSERT(pPds->eBufferType() == BT_COMPILEERRORS);

  Row *pInfoRow = pPds->pFirstRow();

  //Stz* szNodeName  = pInfoRow->szColVal_ofs(0);
  eEC eErrorCode   = (eEC) pInfoRow->iColVal_int(1);
  i8 iLineNo       = pInfoRow->iColVal_int(2);
  i8 iColumnNo     = pInfoRow->iColVal_int(3);
  Stz* szErrorText = pInfoRow->szColVal_ofs(4);

  Stz* szMsgFmt = "Error: %s; Line: %lld; Column: %lld; %s"_Stz;
  vSerialiseErrorResponse(eErrorCode,
                          szMsgFmt,
                          aEC2[eErrorCode],
                          iLineNo,
                          iColumnNo,
                          szErrorText);

  m_oQueryCtx.vIncStmtFailed();
  m_pStmtCtx->vGotError(true);

  // An error at this point ends the current statement
  vNextState(API_STATEMENT_COMPLETED);
}

/*----------------------------------------------------------------------------*/
/* Update the state after an error is received from the execution engine      */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vExecutionError(Pds* pPds)
{
  ASSERT(pPds->eBufferType() == BT_EXECERRORS);

  Row *pInfoRow = pPds->pFirstRow();

  //Stz* szNodeName  = pInfoRow->szColVal_ofs(0);
  eEC eErrorCode   = (eEC) pInfoRow->iColVal_int(1);
  Stz* szErrorText = pInfoRow->szColVal_ofs(2);

  vTraceComponent(TC_WLP, "Client %s: Error infoset received: %s\n"_Stz, m_sClientAddr.szStz(), aErrorMsg[eErrorCode]);

  if (eErrorCode == EC_WL_SOCKET_CLOSED) {
    // Disconnect immediately
    m_eProtocolState = DISCONNECTED;
    return; // No need to serialise and send this.

  } else if (eErrorCode == EC_WL_DISCONNECT_REQ) {
    if (m_pStmtCtx) {
      m_bGotGracefulDisconnect = true;    // Disconnect nicely at the end of the command
      return; // No need to serialise and send this.

    } else {
      // No statement running. Disconnect immediately.
      m_eProtocolState = DISCONNECTED;
      return; // No need to serialise and send this.
    }
  }

  if (!m_pStmtCtx) {
    // This handler is called when a client disconnects. If a statement isn't running then it can be ignored.
    return;
  }

  ASSERT(m_pStmtCtx);

  if (m_pStmtCtx->bGotError()) {
    return;
  }

  if (!bCheckState( {API_STATEMENT_SUBMITTED, API_METADATA_RECEIVED, API_DATA_RECEIVED} )) {
    return;
  }

  Stz* szMsgFmt = "Error: %s; %s"_Stz;
  vSerialiseErrorResponse(eErrorCode,
                          szMsgFmt,
                          aEC2[eErrorCode],
                          szErrorText);

  m_pStmtCtx->vGotError(true);

  // An error at this point ends the current statement
  vNextState(API_STATEMENT_COMPLETED);
}

/*----------------------------------------------------------------------------*/
/* Handle a warning received from the execution engine                        */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vExecutionWarning(Pds* pPdsRowBuffer)
{
  ASSERT(m_pStmtCtx);

  if (!bCheckState( {API_STATEMENT_SUBMITTED, API_METADATA_RECEIVED, API_DATA_RECEIVED} )) {
    return;
  }

  ASSERT(pPdsRowBuffer->eBufferType() == BT_WARNINGS);

  Row *pInfoRow = pPdsRowBuffer->pFirstRow();

  ASSERT(!pInfoRow->bIsColNull(0));

  s2K sReason;
  sReason.vSprint("Warning: %s"_Stz, pInfoRow->szColVal_ofs(0));
  Stz* szSqlState  = pInfoRow->bIsColNull(1) ? nullptr : pInfoRow->szColVal_ofs(1);
  i8   iVendorCode = pInfoRow->bIsColNull(2) ? 0 : pInfoRow->iColVal_int(2);

  vSerialiseWarningResponse(sReason.szStz(),
                            szSqlState,
                            iVendorCode);
}

/*----------------------------------------------------------------------------*/
/* Accumulate any affected row counts for the current statement               */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vAffectedRowCountPds(Pds* pPdsRowBuffer)
{
  if (!bCheckState( {API_STATEMENT_SUBMITTED, API_STATEMENT_COMPLETED} )) {
    return;
  }

  // We've already sent a response to the client. Ignore any further affected row counts.
  // This could occur when affected row counts are sent after an error has
  // already been received.
  if (m_eProtocolState == API_STATEMENT_COMPLETED || !m_pStmtCtx) {
    return;
  }

  ASSERT(m_pStmtCtx);
  ASSERT(pPdsRowBuffer->eBufferType() == BT_AFFECTEDROWS);
  ASSERT(pPdsRowBuffer->nItems() == 1);

  Row *pInfoRow = pPdsRowBuffer->pFirstRow();
  ASSERT(pPdsRowBuffer);

  i8 iRowsProcessed = pInfoRow->iColVal_int(0);
  i8 iRowsInvalid   = pInfoRow->iColVal_int(1);
  i8 iRowsSkipped   = pInfoRow->iColVal_int(2);
  i8 iRowsInserted  = pInfoRow->iColVal_int(3);
  i8 iRowsUpdated   = pInfoRow->iColVal_int(4);
  i8 iRowsDeleted   = pInfoRow->iColVal_int(5);

  m_pStmtCtx->vAddRowsProcessed(iRowsProcessed);
  m_pStmtCtx->vAddRowsSkipped(iRowsSkipped);
  m_pStmtCtx->vAddRowsInvalid(iRowsInvalid);
  m_pStmtCtx->vAddRowsInserted(iRowsInserted);
  m_pStmtCtx->vAddRowsUpdated(iRowsUpdated);
  m_pStmtCtx->vAddRowsDeleted(iRowsDeleted);
}

/*----------------------------------------------------------------------------*/
/* Update the state after the child job has exited                            */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vChildJobExited()
{
  ASSERT(m_pStmtCtx);
  if (!bCheckState( { API_STATEMENT_SUBMITTED,
                      API_METADATA_RECEIVED,
                      API_DATA_RECEIVED,
                      API_STATEMENT_COMPLETED } )) {
    return;
  }

  if (!m_pStmtCtx->bGotError()) {
    // Send any warnings about skipped or invalid rows before we send the
    // StatementResponse message because this terminates the sequence.
    vSerialiseInvalidSkippedRowsWarning();

    // We need to send a StatementResponse message to the client
    m_oQueryCtx.vIncStmtSucceeded();

    i8 nAffectedRows = m_pStmtCtx->nAffectedRows();
    WLP::StatementContext::eAffectedVerb eVerb = m_pStmtCtx->eVerb();

    StatementResponse xMsg;
    xMsg.__set_statementId(m_pStmtCtx->szStmtId()->azChar());
    xMsg.__set_statementString(m_pStmtCtx->szStmt()->azChar());
    xMsg.__set_numMatchedRows(nAffectedRows);
    xMsg.__set_numAffectedRows(nAffectedRows);
    xMsg.__set_affectedVerb(eToWLAffectedVerb(eVerb));

    vSerialiseAndSendMsg(&xMsg, MessageTypes::type::StatementResponse);
  }

  bool bAbortRemaining = (m_eProtocolState == DISCONNECTED) ||
                         (m_pStmtCtx->bGotError() && !m_oQueryCtx.bContinueOnError());

  delete m_pStmtCtx;
  m_pStmtCtx = nullptr;

  if (m_eProtocolState != DISCONNECTED) {
    vNextState(API_CHILD_JOB_EXITED);
  }

  if (bAbortRemaining) {
    m_oQueryCtx.vAbortEarly(true);
  }
}

/*----------------------------------------------------------------------------*/
/* Return true if a child job is currently executing                          */
/*----------------------------------------------------------------------------*/
bool AdaptorWL::bIsChildJobActive()
{
  switch (m_eProtocolState) {
    case API_STATEMENT_SUBMITTED:
    case API_METADATA_RECEIVED:
    case API_DATA_RECEIVED:
    case API_STATEMENT_COMPLETED:
      return true;

    default:
      return false;
  }
}

/*----------------------------------------------------------------------------*/
/* Notify the client about the resultset definition                           */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vResultMetadata(StreamDef* pStreamDef)
{
  ASSERT(m_pStmtCtx);
  if (!bCheckState( {API_STATEMENT_SUBMITTED} )) {
    return;
  }

  vNextState(API_METADATA_RECEIVED);

  if (m_pStmtCtx->bGotError()) {
    return;
  }

  // Check if a resultset was expected for this query
  if (m_oQueryCtx.bMustNotReturnRows()) {
    // This is an error
    m_pStmtCtx->vGotError(true);

    eEC eErrCode = EC_WL_RESULTSET_RETURNED;
    Stz* szErrMsg = "Error: %s; %s: %s"_Stz;
    vSerialiseErrorResponse(eErrCode,
                            szErrMsg,
                            aEC2[eErrCode],
                            aErrorMsg[eErrCode],
                            m_pStmtCtx->szStmt());
    return;
  }

  // Save the stream definition so we can interpret the row data
  m_pStmtCtx->vStreamDef(pStreamDef);

  // Build the list of columns for the thrift message
  std::vector<ColumnMetadata> xColumns;
  i8 nColumns = pStreamDef->nColumns();

  for (i8 i=0; i<nColumns; i++) {
    Column* pColumn = pStreamDef->pGetColumn(i);

    // Convert the SE eCT type to a wireline type
    TypeId xTypeId = pColumn->xColumnTypeId();
    i8 iWLType = xIdToWLType(xTypeId);
//    i8 iWLType = eToWLType(eColType);
    DataType::type eWLType = (DataType::type) iWLType;

    if (iWLType < 0) {
      eEC eErrCode = EC_WL_INTERNAL_ERROR;

      if (!xTypeId.bIsValid()) {
        Stz* szErrMsg = "Error: %s; %s: Could not convert internal type %d to a wireline type"_Stz;
        vSerialiseErrorResponse(eErrCode,
                                szErrMsg,
                                aEC2[eErrCode],
                                aErrorMsg[eErrCode],
                                xTypeId.iIdVal());

      } else {
        Stz* szErrMsg = "Error: %s; %s: Could not convert internal type %s to a wireline type"_Stz;
        vSerialiseErrorResponse(eErrCode,
                                szErrMsg,
                                aEC2[eErrCode],
                                aErrorMsg[eErrCode],
                                xTypeId.szTypeName());
      }

      m_pStmtCtx->vGotError(true);
      vNextState(API_STATEMENT_COMPLETED);
      return;
    }

    ColumnMetadata xColumn{};
    xColumn.__set_columnName(pColumn->szColNm()->azChar());
    xColumn.__set_columnType(eWLType);

    if (pColumn->iColumnLength() > 0)   xColumn.__set_precision(pColumn->iColumnLength());
    if (pColumn->iColumnScale() > 0)    xColumn.__set_scale(pColumn->iColumnScale());

    xColumn.__set_isNullable(pColumn->bIsNullable());

    // TODO: Fill in when supported
//    xColumn.__set_charsetName();
//    xColumn.__set_collationName();
//    xColumn.__set_isAutoIncrement();

    xColumns.push_back(xColumn);
  }

  RowMetadataResponse xRowMetadataResponse;
  xRowMetadataResponse.__set_numColumns(nColumns);
  xRowMetadataResponse.__set_isBoundedResult(true);
  xRowMetadataResponse.__set_columns(xColumns);

  vSerialiseAndSendMsg(&xRowMetadataResponse, MessageTypes::type::RowMetadataResponse);
}

/*----------------------------------------------------------------------------*/
/* Send row data to the client                                                */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vRowData(CrwBuffer* pCrwBuffer)
{
  ASSERT(m_pStmtCtx);
  ASSERT(m_pStmtCtx->pStreamDef());

  if (!bCheckState({API_METADATA_RECEIVED, API_DATA_RECEIVED})) {
    return;
  }

  vNextState(API_DATA_RECEIVED);

  if (m_pStmtCtx->bGotError()) {
    return;
  }

  StreamDef* pStreamDef = m_pStmtCtx->pStreamDef();

  HV_RowSingle hvRowSingle(pStreamDef);

  // Work out how many rows from this CrwBuffer to serialise
  i8 iStartingRow = 0;
  d8 dScalingFactor = 1.0;  // if the serialised form doesn't fit in a single payload buffer
  i8 iRowsInBuf = pCrwBuffer->nRows();
  i8 iMaxRowsThatFit = 0; // how many did we sucessfully fit in a RowDataResponse?

  // We might need to send multiple RowDataResponses for this CrwBuffer.
  // This can be due to a limit of rows per response
  while(iStartingRow < iRowsInBuf) {
    std::vector<com::rapidsdata::wirelineprotocol::messages::v5::Row> xRowList;

    i8 iRowsToSend = nRowsToSend(iRowsInBuf - iStartingRow);
    if (iRowsToSend <= 0) {
      break;
    }

    // If we are sending a smaller number of rows then don't scale the quantity.
    // We don't want the last partial filled RowDataResponse to be split into
    // many more smaller ones.
    if (iMaxRowsThatFit == 0 || iRowsToSend > iMaxRowsThatFit) {
      iRowsToSend = (i8) (iRowsToSend * dScalingFactor + 0.5);
      if (iRowsToSend == 0) {
        iRowsToSend = 1;
      }

    } else {
      // We're sending less rows than we have previously fit. If these don't
      // fit then we ought to restart the scaling once again.
      dScalingFactor = 1.0;
      iMaxRowsThatFit = 0;
    }

    for (i8 i_Crw = iStartingRow; i_Crw < iRowsToSend; i_Crw++) {
      // get the compact row
      Crw* aCrw = (Crw*) pCrwBuffer->aGetHead(i_Crw);

      // ignore deleted rows
      if (aCrw->bIsDeleted()) {
        continue;
      }

      // inflate compact row into a RowSingle
      RowInflator::vInflate_AllColumns_into_RowSingle(hvRowSingle, aCrw);
      Row* pRow = hvRowSingle->pRowCurrent();

      vCreateAndAddRow(&xRowList, pRow, pStreamDef);
    }

    ASSERT((i8) xRowList.size() == iRowsToSend);

    RowDataResponse xRowDataResponse;
    xRowDataResponse.__set_rows(xRowList);

    // Serialise and send. Ensure that the message is < 1MB.
    try {
      vSerialiseAndSendMsg(&xRowDataResponse, MessageTypes::type::RowDataResponse, false);

    } catch (WLP::BufferFullException& e) {

      // Special case where we cannot serialise a single row.
      if (dScalingFactor < 1.0 && iRowsToSend == 1) {
        m_pStmtCtx->vGotError(true);
        eEC eErrCode = EC_WL_ROW_TOO_BIG;
        Stz* szErrMsg = "Error: %s; %s; %lld bytes"_Stz;
        vSerialiseErrorResponse(eErrCode,
                                szErrMsg,
                                aEC2[eErrCode],
                                aErrorMsg[eErrCode],
                                WLP::PayloadObj::C_MAX_PAYLOAD_BYTES);
        return;
      }

      dScalingFactor *= C_ROWDATARESP_SCALE_DOWN;
      vTraceComponent(TC_WLP, "Client %s: RowDataResponse cannot fit %lld rows\n"_Stz, m_sClientAddr.szStz(), iRowsToSend);
      continue;
    }

    vTraceComponent(TC_WLP, "Client %s: RowDataResponse serialised %lld rows\n"_Stz, m_sClientAddr.szStz(), iRowsToSend);
    iStartingRow += iRowsToSend;

    if (iRowsToSend > iMaxRowsThatFit) {
      iMaxRowsThatFit = iRowsToSend;
    }

    // Slowly increase the scaling factor
    dScalingFactor += C_ROWDATARESP_SCALE_UP;
    if (dScalingFactor > 1.0) {
      dScalingFactor = 1.0;
    }

    m_pStmtCtx->vAddRowsSelected(iRowsToSend);
  }
}

/*----------------------------------------------------------------------------*/
/* Send row data to the client                                                */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vPdsRowData(Pds* pPdsRowBuffer)
{
  ASSERT(m_pStmtCtx);
  ASSERT(m_pStmtCtx->pStreamDef());

  if (!bCheckState({API_METADATA_RECEIVED, API_DATA_RECEIVED})) {
    return;
  }

  vNextState(API_DATA_RECEIVED);

  if (m_pStmtCtx->bGotError()) {
    return;
  }

  StreamDef* pStreamDef = m_pStmtCtx->pStreamDef();

  // Work out how many rows from this RowBuffer to serialise
  i8 iStartingRow = 0;
  d8 dScalingFactor = 1.0;  // if the serialised form doesn't fit in a single payload buffer
  i8 iRowsInBuf = pPdsRowBuffer->nItems();
  i8 iMaxRowsThatFit = 0; // how many did we sucessfully fit in a RowDataResponse?

  // We might need to send multiple RowDataResponses for this RowBuffer.
  // This can be due to a limit of rows per response
  while(iStartingRow < iRowsInBuf) {
    std::vector<com::rapidsdata::wirelineprotocol::messages::v5::Row> xRowList;

    i8 iRowsToSend = nRowsToSend(iRowsInBuf - iStartingRow);
    if (iRowsToSend <= 0) {
      break;
    }

    // If we are sending a smaller number of rows then don't scale the quantity.
    // We don't want the last partial filled RowDataResponse to be split into
    // many more smaller ones.
    if (iMaxRowsThatFit == 0 || iRowsToSend > iMaxRowsThatFit) {
      iRowsToSend = (i8) (iRowsToSend * dScalingFactor + 0.5);
      if (iRowsToSend == 0) {
        iRowsToSend = 1;
      }

    } else {
      // We're sending less rows than we have previously fit. If these don't
      // fit then we ought to restart the scaling once again.
      dScalingFactor = 1.0;
      iMaxRowsThatFit = 0;
    }

    // Find the SkipListBlock entry containing the row
    SkipListBlock* pSkipListBlock = pPdsRowBuffer->pFind(&iStartingRow);
    RASSERT(pSkipListBlock);

    // Init a PdsCursor, and start iterating at this entry
    PdsCursor* pPdsCursor = new PdsCursor(pPdsRowBuffer, pSkipListBlock);


    for (i8 i_Row = iStartingRow; i_Row < iStartingRow + iRowsToSend; i_Row++) {
      Row* pRow = pPdsCursor->pCurrRow();
      RASSERT(pRow);
      vCreateAndAddRow(&xRowList, pRow, pStreamDef);
      pPdsCursor->vNext();
    }

    pPdsCursor->m_pPds = nullptr;
    delete pPdsCursor;

    ASSERT( (i8) xRowList.size() == iRowsToSend);

    RowDataResponse xRowDataResponse;
    xRowDataResponse.__set_rows(xRowList);

    // Serialise and send. Ensure that the message is < 1MB.
    try {
      vSerialiseAndSendMsg(&xRowDataResponse, MessageTypes::type::RowDataResponse, false);

    } catch (WLP::BufferFullException& e) {

      // Special case where we cannot serialise a single row.
      if (dScalingFactor < 1.0 && iRowsToSend == 1) {
        m_pStmtCtx->vGotError(true);
        eEC eErrCode = EC_WL_ROW_TOO_BIG;
        Stz* szErrMsg = "Error: %s; %s; %ldd bytes"_Stz;
        vSerialiseErrorResponse(eErrCode,
                                szErrMsg,
                                aEC2[eErrCode],
                                aErrorMsg[eErrCode],
                                WLP::PayloadObj::C_MAX_PAYLOAD_BYTES);
        return;
      }

      dScalingFactor *= C_ROWDATARESP_SCALE_DOWN;
      vTraceComponent(TC_WLP, "Client %s: RowDataResponse cannot fit %lld rows\n"_Stz, m_sClientAddr.szStz(), iRowsToSend);
      continue;
    }

    vTraceComponent(TC_WLP, "Client %s: RowDataResponse serialised %lld rows\n"_Stz, m_sClientAddr.szStz(), iRowsToSend);
    iStartingRow += iRowsToSend;

    if (iRowsToSend > iMaxRowsThatFit) {
      iMaxRowsThatFit = iRowsToSend;
    }

    // Slowly increase the scaling factor
    dScalingFactor += C_ROWDATARESP_SCALE_UP;
    if (dScalingFactor > 1.0) {
      dScalingFactor = 1.0;
    }

    m_pStmtCtx->vAddRowsSelected(iRowsToSend);
  }
}

/*----------------------------------------------------------------------------*/
/* Send a QueryExecuteResponse, update the state & clean up the current query */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vQueryComplete()
{
  if (!bCheckState( { QUERY_BEGUN,
                      API_CHILD_JOB_EXITED } )) {
    return;
  }

  eState eLastState = m_eProtocolState;
  vNextState(QUERY_COMPLETED);

  if (eLastState != QUERY_BEGUN) {
    u8 uQueryEndNanos = Hardware::uReadClock();
    double dQueryTime = (double) (uQueryEndNanos - m_oQueryCtx.uQueryStartNanos()) / C_1B;

    QueryExecuteResponse xResponse;
    xResponse.executionTime = dQueryTime;
    xResponse.numStatementsParsed = m_oQueryCtx.nStmtParsed();
    xResponse.numStatementsSucceeded = m_oQueryCtx.nStmtSucceeded();
    xResponse.numStatementsFailed = m_oQueryCtx.nStmtFailed();

    vSerialiseAndSendMsg(&xResponse, MessageTypes::type::QueryExecuteResponse);
  }

  // Doing this after changing the state to QUERY_COMPLETED will delete m_pCurrentMsg
  m_oQueryCtx.vReset();


  if (m_bGotGracefulDisconnect) {
    // Disconnect the client gracefully after sending the query dialog completion
    m_eProtocolError = EC_WL_DISCONNECT_REQ;
    m_eProtocolState = DISCONNECTED;

  } else {
    vNextState(READY);
  }
}

/*----------------------------------------------------------------------------*/
/* Processed parsed tasks and send a QueryReceiptResponse back to the client  */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vQueryReceipt(ENGINE::TaskList* pTaskList)
{
  if (!bCheckState( {QUERY_BEGUN} )) {
    return;
  }

  QueryReceiptResponse xQueryReceipt;
  std::vector<StatementReceipt> xStatementReceipts;

  i8 nStatements = 0;
  UNUSED(nStatements);

  pTaskList->vGrabLock();
  Task* pTask = pTaskList->pPeekTask();
  pTaskList->vDropLock();

  while (pTask != nullptr) {
    ++nStatements;

    Stz* szCommand = pTask->pJob()->szCmdText();
    s256 sJobId;
    sJobId.vSprint("%lld"_Stz, pTask->pJob()->tJobId().iAsI8());

    StatementReceipt xStatementReceipt{};
    xStatementReceipt.__set_statementString(szCommand->azChar());
    xStatementReceipt.__set_statementId(sJobId.azChar());
    xStatementReceipt.__set_statementKey(12345);    // TODO: set this appropriately

    xStatementReceipts.push_back(xStatementReceipt);

    // TODO: Register the statement ID and statement key

    pTask = pTask->pNextItem();
  }

  xQueryReceipt.__set_statementReceipts(xStatementReceipts);
  vSerialiseAndSendMsg(&xQueryReceipt, MessageTypes::QueryReceiptResponse);
}

/*----------------------------------------------------------------------------*/
/*-- Private -----------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
/* Return the next PayloadObj to be sent                                      */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vDeserialiseMsg(i8 iMessageType)
{
  MessageTypes::type eMsgType = (MessageTypes::type) iMessageType;
  ASSERT(!m_pCurrentMsg);
  ASSERT(m_iCurrentMsgType == -1);

  switch (eMsgType) {

    case MessageTypes::type::ProtocolCompatibleRequest:      m_pCurrentMsg = new ProtocolCompatibleRequest();      break;
    case MessageTypes::type::DisconnectRequest:              m_pCurrentMsg = new DisconnectRequest();              break;
    case MessageTypes::type::PingRequest:                    m_pCurrentMsg = new PingRequest();                    break;
    case MessageTypes::type::ClearTextTunnelRequest:         m_pCurrentMsg = new ClearTextTunnelRequest();         break;
    case MessageTypes::type::PlainAuthenticationRequest:     m_pCurrentMsg = new PlainAuthenticationRequest();     break;
    case MessageTypes::type::KerberosAuthenticationRequest:  m_pCurrentMsg = new KerberosAuthenticationRequest();  break;
    case MessageTypes::type::GssNegotiationMsg:              m_pCurrentMsg = new GssNegotiationMsg();              break;
    case MessageTypes::type::SessionChangeRequest:           m_pCurrentMsg = new SessionChangeRequest();           break;
    case MessageTypes::type::QueryExecuteRequest:            m_pCurrentMsg = new QueryExecuteRequest();            break;
    case MessageTypes::type::QueryProgressRequest:           m_pCurrentMsg = new QueryProgressRequest();           break;
    case MessageTypes::type::SessionMonitoringRequest:       m_pCurrentMsg = new SessionMonitoringRequest();       break;
    case MessageTypes::type::ParseStatementRequest:          m_pCurrentMsg = new ParseStatementRequest();          break;
    case MessageTypes::type::StatementBindAndExecuteRequest: m_pCurrentMsg = new StatementBindAndExecuteRequest(); break;
    case MessageTypes::type::ClosePreparedStatementRequest:  m_pCurrentMsg = new ClosePreparedStatementRequest();  break;

    // The following are messages that should never be received by RapidsSE
    // as they should never be sent by a client. Only sent by a server.
    //   ProtocolCompatibleResponse:
    //   ProtocolIncompatibleResponse:
    //   ErrorResponse:
    //   WarningResponse:
    //   ShutdownNotification:
    //   DisconnectNotification:
    //   PingResponse:
    //   TunnelSupportedResponse:
    //   AuthenticationOkResponse:
    //   SessionChangeResponse:
    //   QueryReceiptResponse:
    //   RowMetadataResponse:
    //   RowDataResponse:
    //   StatementResponse:
    //   QueryExecuteResponse:
    //   QueryProgressResponse:
    //   SessionProtocolVersionResponse:
    //   SessionDataResponse:
    //   SessionDroppedMessagesResponse:
    //   SessionMonitoringResponse:
    //   ParseStatementResponse:
    //   ClosePreparedStatementResponse:

    default:
      // Respond to the client with an error, and set the m_eProtocolError
      // which will cause the socket to be closed.
      std::string sMsgType = to_string(eMsgType);
      eEC eErrCode = EC_WL_PROTOCOL_ERROR;
      vSerialiseErrorResponse(eErrCode,
                              "Error: %s; %s: Received an unexpected wireline message type of %s."_Stz,
                              aEC2[eErrCode],
                              aErrorMsg[eErrCode],
                              sMsgType.c_str());

      vDisconnectClient(eErrCode);
      return;
  }

  // Set the current message type and decode the message
  m_iCurrentMsgType = eMsgType;
  i8 iBytesRead;

  try {
    iBytesRead = m_pCurrentMsg->read(m_pProtocol.get());

  } catch (apache::thrift::transport::TTransportException& e) {
    // Respond to the client with an error, and set the m_eProtocolError
    // which will cause the socket to be closed.
    std::string sMsgType = to_string(eMsgType);
    eEC eErrCode = EC_WL_PROTOCOL_ERROR;
    vSerialiseErrorResponse(eErrCode,
                            "Error: %s; %s: Unable to decode wireline message of type %s: %s"_Stz,
                            aEC2[eErrCode],
                            aErrorMsg[eErrCode],
                            sMsgType.c_str(),
                            e.what());

    vDisconnectClient(eErrCode);
    return;
  }

  if (iBytesRead == 0) {
    // Respond to the client with an error, and set the m_eProtocolError
    // which will cause the socket to be closed.
    std::string sMsgType = to_string(eMsgType);
    eEC eErrCode = EC_WL_PROTOCOL_ERROR;
    vSerialiseErrorResponse(eErrCode,
                            "Error: %s; %s: Unable to decode wireline message of type %s."_Stz,
                            aEC2[eErrCode],
                            aErrorMsg[eErrCode],
                            sMsgType.c_str());

    vDisconnectClient(eErrCode);
    return;
  }

  vTraceComponent(TC_WLP, "Client %s: Decoded a wireline message of type: %s\n"_Stz, m_sClientAddr.szStz(), to_string(eMsgType).c_str());
}

/*----------------------------------------------------------------------------*/
/* Deallocates the current message                                            */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vCleanupCurrentMsg()
{
  delete m_pCurrentMsg;
  m_pCurrentMsg = nullptr;
  m_iCurrentMsgType = -1;
}


/*----------------------------------------------------------------------------*/
/* Serialises a thrift ErrorResponse with a given message.                    */
/* Adds the serialised message to the output queue.                           */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vSerialiseErrorResponse(eEC eErrorCode, Stz* sFormat, ...)
{
  // First, serialise and send any warnings because sending the error will
  // terminate the statement execution.
  vSerialiseInvalidSkippedRowsWarning();

  va_list args;

  // Create the error message
  s16KB sBuf;

  va_start(args,sFormat);
  vsnprintf(sBuf.azChar(), sBuf.nSpace(), sFormat->azChar(),args);
  va_end(args);


  // Create and populate the error response object to be serialised
  ErrorResponse oErrorResponse;
  oErrorResponse.__set_errorString(sBuf.azChar());

  if (eErrorCode != EC_NONE) {
    oErrorResponse.__set_errorCode(eErrorCode);
  }

  // Serialise the error message to a payload and add it to the output queue
  vSerialiseAndSendMsg(&oErrorResponse, MessageTypes::ErrorResponse);
}

/*----------------------------------------------------------------------------*/
/* Serialises a thrift WarningResponse with a given message.                  */
/* Adds the serialised message to the output queue.                           */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vSerialiseWarningResponse(Stz* szReason, Stz* szSqlState, i8 iVendorCode)
{
  // Create and populate the warning response object to be serialised
  WarningResponse oWarningResponse;
  oWarningResponse.__set_warningString(szReason->azChar());

  if (szSqlState) {
    oWarningResponse.__set_sqlStateCode(szSqlState->azChar());
  }

  oWarningResponse.__set_warningCode(iVendorCode);

  // Serialise the error message to a payload and add it to the output queue
  vSerialiseAndSendMsg(&oWarningResponse, MessageTypes::WarningResponse);
}

/*----------------------------------------------------------------------------*/
/* Serialises a thrift WarningResponse with a message about the number of rows*/
/* that were skipped or invalid.                                              */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vSerialiseInvalidSkippedRowsWarning()
{
  if (!m_pStmtCtx || m_eProtocolState == DISCONNECTED) {
    return;
  }

  // Send a warning if there are rows skipped or invalid.
  i8 iRowsInvalid = m_pStmtCtx->nRowsInvalid();
  i8 iRowsSkipped = m_pStmtCtx->nRowsSkipped();

  if (iRowsSkipped > 0 || iRowsInvalid > 0) {
    s2K sReason = "Warning: "_Stz;

    if (iRowsInvalid > 0) {
      sReason.vCatSprint("%lld rows were invalid"_Stz, iRowsInvalid);

      if (iRowsSkipped > 0) {
        sReason.vConcat(" and "_Stz);
      }
    }

    if (iRowsSkipped > 0) {
      sReason.vCatSprint("%lld rows were skipped"_Stz, iRowsSkipped);
    }

    vSerialiseWarningResponse(sReason.szStz(),
                              nullptr,
                              0);
  }
}

/*----------------------------------------------------------------------------*/
/* Serialises an abstract thrift message and adds it to the output queue.     */
/* Throws a WLP::BufferFullException if the message doesn't fit and           */
/* bHandleBufferFull is false. Otherwise it serialises an ErrorMsg & closes   */
/* the socket.                                                                */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vSerialiseAndSendMsg(apache::thrift::TBase* pMsg, i8 iMsgType, bool bHandleBufferFull)
{
  MessageTypes::type eMsgType = (MessageTypes::type) iMsgType;

  WLP::PayloadObj* pPayload = new WLP::PayloadObj();

  // Serialise the error message to a payload
  m_pTransport->vUseBuffer(pPayload->pcBuf(), WLP::PayloadObj::C_MAX_PAYLOAD_BYTES);
  WLP::WlpHeader* pHeader = m_pTransport->pAllocHeader(eMsgType);

  try {
    pMsg->write(m_pProtocol.get());

  } catch (WLP::BufferFullException& e) {

    m_pTransport->vRemoveBuffer();
    delete pPayload;

    if (!bHandleBufferFull) {
      // We expect this exception to be caught elsewhere.
      throw;
    }

    // Handle this situation by sending an error message and causing the client socket
    // to be closed.
    eEC eErrCode = EC_WL_MSG_TOO_BIG;
    s2K sBuf;
    sBuf.vSprint("Error: %s; %s;"_Stz, aEC2[eErrCode], aState[m_eProtocolState]);

    ErrorResponse oErrorResponse;
    oErrorResponse.__set_errorString(sBuf.azChar());
    oErrorResponse.__set_errorCode(eErrCode);

    // Recurse, but importantly, if this fails then we are sweet out of luck.
    try {
      vSerialiseAndSendMsg(&oErrorResponse, MessageTypes::ErrorResponse, false);

    } catch (WLP::BufferFullException& e) {
      vTrace("Unable to serialise a simple ErrorResponse about a buffer being full.\n"_Stz);
      // Client will be disconnected anyway.
    }

    vDisconnectClient(eErrCode);
    return;
  }

  // Message size needs to be written into the header of the message
  // as well as the payload object.
  i8 iMsgSize = m_pTransport->iBytesWritten();
  pHeader->vMsgSize(iMsgSize);
  pPayload->vSetPayloadSize(iMsgSize);
  m_pTransport->vRemoveBuffer();

  // Add it to the send list
  m_pPayloadSendList->vAppend(pPayload);
}

/*----------------------------------------------------------------------------*/
/* Change the state and check the state transition                            */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vNextState(eState eNextState)
{
  typedef std::initializer_list<eState> StateList;

  // The list of valid next states from a given state.
  // There is a state transition diagram that describes the valid transitions between
  // these states on the Sharepoint fileserver under the Wire Line Protocol directory.
  /*01*/ static StateList aNextStates_Connected          = { PROTOCOL_COMPATIBLE, DISCONNECTED };
  /*02*/ static StateList aNextStates_ProtocolCompatible = { TUNNELED, DISCONNECTED };
  /*03*/ static StateList aNextStates_Tunneled           = { AUTHENTICATED, KERBEROS_NEGOTIATING, DISCONNECTED };
  /*04*/ static StateList aNextStates_KerberosAuth       = { AUTHENTICATED, DISCONNECTED };
  /*05*/ static StateList aNextStates_Authenticated      = { READY, DISCONNECTED };
  /*06*/ static StateList aNextStates_Ready              = { READY, QUERY_BEGUN, DISCONNECTED };
  /*07*/ static StateList aNextStates_QueryBegun         = { API_STATEMENT_TO_SUBMIT, QUERY_COMPLETED, DISCONNECTED };
  /*08*/ static StateList aNextStates_ApiStmtToSubmit    = { API_STATEMENT_SUBMITTED, API_CHILD_JOB_EXITED, DISCONNECTED };
  /*08*/ static StateList aNextStates_ApiStmtSubmitted   = { API_METADATA_RECEIVED, API_STATEMENT_COMPLETED, API_CHILD_JOB_EXITED };
  /*09*/ static StateList aNextStates_ApiMetadataRcvd    = { API_DATA_RECEIVED, API_STATEMENT_COMPLETED, API_CHILD_JOB_EXITED };
  /*10*/ static StateList aNextStates_ApiDataRcvd        = { API_DATA_RECEIVED, API_STATEMENT_COMPLETED, API_CHILD_JOB_EXITED };
  /*11*/ static StateList aNextStates_ApiStmtCompleted   = { API_CHILD_JOB_EXITED };
  /*11*/ static StateList aNextStates_ApiChildJobExited  = { QUERY_COMPLETED, API_STATEMENT_TO_SUBMIT, DISCONNECTED };
  /*12*/ static StateList aNextStates_QueryCompleted     = { READY, DISCONNECTED };
  /*13*/ static StateList aNextStates_Disconnected       = { DISCONNECTED };

  // We'll use a reference to one of these static initializer lists above.
  std::initializer_list<eState> eValidNextStates = aNextStates_Connected;

  switch(m_eProtocolState) {
    /*01*/ case CONNECTED:               eValidNextStates = aNextStates_Connected;           break;
    /*02*/ case PROTOCOL_COMPATIBLE:     eValidNextStates = aNextStates_ProtocolCompatible;  break;
    /*03*/ case TUNNELED:                eValidNextStates = aNextStates_Tunneled;            break;
    /*04*/ case KERBEROS_NEGOTIATING:    eValidNextStates = aNextStates_KerberosAuth;        break;
    /*05*/ case AUTHENTICATED:           eValidNextStates = aNextStates_Authenticated;       break;
    /*06*/ case READY:                   eValidNextStates = aNextStates_Ready;               break;
    /*07*/ case QUERY_BEGUN:             eValidNextStates = aNextStates_QueryBegun;          break;
    /*08*/ case API_STATEMENT_TO_SUBMIT: eValidNextStates = aNextStates_ApiStmtToSubmit;     break;
    /*09*/ case API_STATEMENT_SUBMITTED: eValidNextStates = aNextStates_ApiStmtSubmitted;    break;
    /*10*/ case API_METADATA_RECEIVED:   eValidNextStates = aNextStates_ApiMetadataRcvd;     break;
    /*11*/ case API_DATA_RECEIVED:       eValidNextStates = aNextStates_ApiDataRcvd;         break;
    /*12*/ case API_STATEMENT_COMPLETED: eValidNextStates = aNextStates_ApiStmtCompleted;    break;
    /*12*/ case API_CHILD_JOB_EXITED:    eValidNextStates = aNextStates_ApiChildJobExited;   break;
    /*13*/ case QUERY_COMPLETED:         eValidNextStates = aNextStates_QueryCompleted;      break;
    /*14*/ case DISCONNECTED:            eValidNextStates = aNextStates_Disconnected;        break;
    default:
      NEVER_REACH
  }

  // Check the transition
  bool bMatched = false;
  for (eState eValidNextState : eValidNextStates) {
    if (eNextState == eValidNextState) {
      bMatched = true;
      break;
    }
  }

  if (!bMatched) {
    // Connection is no longer viable
    vTrace("Wireline client is making an invalid transition from state %s to %s\n"_Stz,
           aState[m_eProtocolState], aState[eNextState]);
    vErrorStateTransition(aState[m_eProtocolState], aState[eNextState]);

    vDisconnectClient(EC_WL_PROTOCOL_ERROR);
    return;
  }

  // Do the transition
  vTraceComponent(TC_WLP, "Client %s: Wireline client transitioning from state %s to state %s\n"_Stz,
         m_sClientAddr.szStz(),
         aState[m_eProtocolState],
         aState[eNextState]);

  m_eProtocolState = eNextState;
}

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -*/
/* Message Handlers Here                                                      */

/*----------------------------------------------------------------------------*/
/* Processes a ProtocolCompatibleRequest message                              */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vProcessProtocolCompatibleRequest()
{
  ASSERT(m_pCurrentMsg);
  ASSERT(m_iCurrentMsgType == MessageTypes::type::ProtocolCompatibleRequest);

  if (!bCheckState({CONNECTED})) { return; }

  ProtocolCompatibleRequest* pMsg = dynamic_cast<ProtocolCompatibleRequest*>(m_pCurrentMsg);
  if (!pMsg) {
    vErrorDynamicCast(to_string(MessageTypes::type::ProtocolCompatibleRequest).c_str());
    return;
  }


  // Create the server build info, required for any response here.
  s2K sBuildInfo;
  sBuildInfo.vSprint("RapidsSE build information:\n" \
                                "  version     %s\n" \
                                "  from branch %s\n" \
                                "  by          %s\n" \
                                "  on          %s\n" \
                                "  at          %s"_Stz,
                                C_VERSION_STRING,
                                C_BUILD_BRANCH,
                                C_BUILD_USERNAME,
                                C_BUILD_DATE,
                                C_BUILD_HOSTNAME );

  // Check if the client is speaking the same protocol version
  i8 iClientProtocolVer = pMsg->clientProtocolVersion;

  if (iClientProtocolVer < C_MIN_SUPPORT_PROTOCOL_VER ||
      iClientProtocolVer > C_MAX_SUPPORT_PROTOCOL_VER) {

    // Serialise a ProtocolIncompatibleResponse

    s256 sErrMsg;
    const eEC eErrType = EC_WL_PROTOCOL_VER_NOT_SUPPORTED;

    if (C_MIN_SUPPORT_PROTOCOL_VER == C_MAX_SUPPORT_PROTOCOL_VER) {
      sErrMsg.vSprint("%s: client supports %lld, server supports version %lld."_Stz,
                      aErrorMsg[eErrType],
                      iClientProtocolVer,
                      C_MIN_SUPPORT_PROTOCOL_VER);
    } else {
      sErrMsg.vSprint("%s: client sent %lld, server supports versions %lld to %lld."_Stz,
                      aErrorMsg[eErrType],
                      iClientProtocolVer,
                      C_MIN_SUPPORT_PROTOCOL_VER,
                      C_MAX_SUPPORT_PROTOCOL_VER);
    }

    ProtocolIncompatibleResponse oIncompatibleResponse;
    oIncompatibleResponse.__set_clientProtocolVersion((int32_t) iClientProtocolVer);
    oIncompatibleResponse.__set_minimumProtocolVersion(C_MIN_SUPPORT_PROTOCOL_VER);
    oIncompatibleResponse.__set_maximumProtocolVersion(C_MAX_SUPPORT_PROTOCOL_VER);
    oIncompatibleResponse.__set_serverSoftwareVersion(const_cast<Stz*>(C_VERSION_STRING)->azChar());
    oIncompatibleResponse.__set_serverBuildInfo(sBuildInfo.azChar());
    oIncompatibleResponse.__set_errorMessage(sErrMsg.azChar());

    vSerialiseAndSendMsg(&oIncompatibleResponse, MessageTypes::type::ProtocolIncompatibleResponse);

    vDisconnectClient(EC_WL_PROTOCOL_VER_NOT_SUPPORTED);
    vNextState(DISCONNECTED);
    return;
  }


  // Serialise a ProtocolCompatibleResponse

  ProtocolCompatibleResponse oCompatibleResponse;
  oCompatibleResponse.__set_protocolVersion((int32_t) iClientProtocolVer);
  oCompatibleResponse.__set_serverSoftwareVersion(const_cast<Stz*>(C_VERSION_STRING)->azChar());
  oCompatibleResponse.__set_serverBuildInfo(sBuildInfo.azChar());

  vSerialiseAndSendMsg(&oCompatibleResponse, MessageTypes::type::ProtocolCompatibleResponse);

  vNextState(PROTOCOL_COMPATIBLE);
}

/*----------------------------------------------------------------------------*/
/* Processes a DisconnectRequest message                                      */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vProcessDisconnectRequest()
{
  ASSERT(m_pCurrentMsg);
  ASSERT(m_iCurrentMsgType == MessageTypes::type::DisconnectRequest);

  // Nothing to do in this message. We set m_eProtocolError to cause the socket
  // to be closed.

  vDisconnectClient(EC_WL_SOCKET_CLOSED);
  vNextState(DISCONNECTED);
}

/*----------------------------------------------------------------------------*/
/* Processes a PingRequest message                                            */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vProcessPingRequest()
{
  ASSERT(m_pCurrentMsg);
  ASSERT(m_iCurrentMsgType == MessageTypes::type::PingRequest);

  if (!bCheckState({READY})) { return; }

  PingRequest* pMsg = dynamic_cast<PingRequest*>(m_pCurrentMsg);
  if (!pMsg) {
    vErrorDynamicCast(to_string(MessageTypes::type::PingRequest).c_str());
    return;
  }

  // Serialise a PingResponse. It has no fields to set.
  PingResponse oPingResponse;
  vSerialiseAndSendMsg(&oPingResponse, MessageTypes::type::PingResponse);

  vNextState(READY);
}

/*----------------------------------------------------------------------------*/
/* Processes a ClearTextTunnelRequest message                                 */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vProcessClearTextTunnelRequest()
{
  ASSERT(m_pCurrentMsg);
  ASSERT(m_iCurrentMsgType == MessageTypes::type::ClearTextTunnelRequest);

  if (!bCheckState({PROTOCOL_COMPATIBLE})) { return; }

  ClearTextTunnelRequest* pMsg = dynamic_cast<ClearTextTunnelRequest*>(m_pCurrentMsg);
  if (!pMsg) {
    vErrorDynamicCast(to_string(MessageTypes::type::ClearTextTunnelRequest).c_str());
    return;
  }

  // Send a TunnelSupportedResponse. No fields to set.
  TunnelSupportedResponse oTunnelResponse;
  vSerialiseAndSendMsg(&oTunnelResponse, MessageTypes::type::TunnelSupportedResponse);

  vNextState(TUNNELED);
}

/*----------------------------------------------------------------------------*/
/* Processes a PlainAuthenticationRequest message                             */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vProcessPlainAuthenticationRequest()
{
  ASSERT(m_pCurrentMsg);
  ASSERT(m_iCurrentMsgType == MessageTypes::type::PlainAuthenticationRequest);

  if (!bCheckState({TUNNELED})) { return; }

  PlainAuthenticationRequest* pMsg = dynamic_cast<PlainAuthenticationRequest*>(m_pCurrentMsg);
  if (!pMsg) {
    vErrorDynamicCast(to_string(MessageTypes::type::PlainAuthenticationRequest).c_str());
    return;
  }

  // At this point we aren't supporting authentication, so it doesn't matter
  // what credentials are passed to in the message.

  // Send a AuthenticationOkResponse. No fields to set.
  AuthenticationOkResponse oAuthResponse;
  vSerialiseAndSendMsg(&oAuthResponse, MessageTypes::type::AuthenticationOkResponse);

  vNextState(AUTHENTICATED);
}

/*----------------------------------------------------------------------------*/
/* Processes a KerberosAuthenticationRequest message                          */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vProcessKerberosAuthenticationRequest()
{
  ASSERT(m_pCurrentMsg);
  ASSERT(m_iCurrentMsgType == MessageTypes::type::KerberosAuthenticationRequest);

  if (!bCheckState({TUNNELED})) { return; }

  KerberosAuthenticationRequest* pMsg = dynamic_cast<KerberosAuthenticationRequest*>(m_pCurrentMsg);
  if (!pMsg) {
    vErrorDynamicCast(to_string(MessageTypes::type::KerberosAuthenticationRequest).c_str());
    return;
  }

  // TODO: populate

  vErrorFeatureNotSupported("Kerberos authentication"_Stz);

  vDisconnectClient(EC_WL_FEATURE_NOT_SUPPORTED);
  vNextState(DISCONNECTED);

  //vNextState(KERBEROS_NEGOTIATING);
}

/*----------------------------------------------------------------------------*/
/* Processes a GssNegotiationMsg message                                      */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vProcessGssNegotiationMsg()
{
  ASSERT(m_pCurrentMsg);
  ASSERT(m_iCurrentMsgType == MessageTypes::type::GssNegotiationMsg);

  if (!bCheckState({KERBEROS_NEGOTIATING})) { return; }

  GssNegotiationMsg* pMsg = dynamic_cast<GssNegotiationMsg*>(m_pCurrentMsg);
  if (!pMsg) {
    vErrorDynamicCast(to_string(MessageTypes::type::GssNegotiationMsg).c_str());
    return;
  }

  // TODO: populate

  vErrorFeatureNotSupported("Kerberos authentication"_Stz);

  vDisconnectClient(EC_WL_FEATURE_NOT_SUPPORTED);
  vNextState(DISCONNECTED);

//  vNextState(AUTHENTICATED);
}

/*----------------------------------------------------------------------------*/
/* Processes a SessionChangeRequest message                                   */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vProcessSessionChangeRequest()
{
  ASSERT(m_pCurrentMsg);
  ASSERT(m_iCurrentMsgType == MessageTypes::type::SessionChangeRequest);

  if (!bCheckState({AUTHENTICATED})) { return; }

  SessionChangeRequest* pMsg = dynamic_cast<SessionChangeRequest*>(m_pCurrentMsg);
  if (!pMsg) {
    vErrorDynamicCast(to_string(MessageTypes::type::SessionChangeRequest).c_str());
    return;
  }

  const char* pcDefaultCatalog = nullptr;
  const char* pcDefaultSchema  = nullptr;
  UNUSED(pcDefaultCatalog);
  UNUSED(pcDefaultSchema);
  // Note: we ignore the defaultFederation field in this message. Doesn't apply to SE.

  if (pMsg->__isset.defaultCatalog) {
    pcDefaultCatalog = pMsg->defaultCatalog.c_str();
  }

  if (pMsg->__isset.defaultSchema) {
    pcDefaultSchema = pMsg->defaultSchema.c_str();
  }

  // TODO: do something with the default catalog and schema


  // Send a SessionChangeResponse.
  SessionChangeResponse oSsnResponse;
  oSsnResponse.sessionId = "ABCD";    // TODO
  oSsnResponse.sessionKey = 1234;     // TODO
  vSerialiseAndSendMsg(&oSsnResponse, MessageTypes::type::SessionChangeResponse);

  vNextState(READY);
}

/*----------------------------------------------------------------------------*/
/* Processes a QueryExecuteRequest message                                    */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vProcessQueryExecuteRequest()
{
  ASSERT(m_pCurrentMsg);
  ASSERT(m_iCurrentMsgType == MessageTypes::type::QueryExecuteRequest);
  ASSERT(!m_pStmtCtx);
  ASSERT(m_oQueryCtx.nStmtParsed() == 0);
  ASSERT(m_oQueryCtx.nStmtSucceeded() == 0);
  ASSERT(m_oQueryCtx.nStmtFailed() == 0);

  if (!bCheckState({READY})) { return; }

  QueryExecuteRequest* pMsg = dynamic_cast<QueryExecuteRequest*>(m_pCurrentMsg);
  if (!pMsg) {
    vErrorDynamicCast(to_string(MessageTypes::type::QueryExecuteRequest).c_str());
    return;
  }

  // Sanity check the parameters in the message
  eEC eErrorCode = EC_NONE;

  // Check if the query string to too large for the sen variable stack
  // and downstream parsing, which places it into an s48K.
  if (pMsg->query.length() + 1 > C_MAX_QUERY_STR) {
    eErrorCode = EC_STATEMENT_TOO_LONG;
    Stz* szFmt = "Error: %s; %s: Statement is %lld bytes; max length is %lld bytes: %s"_Stz;

    s256 sStmtBuf;
    sStmtBuf.vCopy((Stz*) pMsg->query.c_str(), 250);
    sStmtBuf.vConcat("..."_Stz);

    vSerialiseErrorResponse(eErrorCode,
                            szFmt,
                            aEC2[eErrorCode],
                            aErrorMsg[eErrorCode],
                            pMsg->query.length() + 1,
                            C_MAX_QUERY_STR,
                            sStmtBuf.szStz());
    vNextState(READY);
    return;
  }

  if (pMsg->rowsPerRowDataResponse < 0) {
    // Sending this error before the QueryReceiptResponse ends the dialog
    eErrorCode = EC_WL_PROTOCOL_ERROR;
    Stz* szMsg = "Error: %s; %s; Message QueryExecuteRequest field rowsPerRowDataResponse cannot be negative. Current value: %lld"_Stz;
    vSerialiseErrorResponse(eErrorCode,
                            szMsg,
                            aEC2[eErrorCode],
                            aErrorMsg[eErrorCode],
                            pMsg->rowsPerRowDataResponse);
    vDisconnectClient(eErrorCode);
    vNextState(READY);
    return;
  }

  if (pMsg->maxRowsToReturn < -1) {
    // Sending this error before the QueryReceiptResponse ends the dialog
    eErrorCode = EC_WL_PROTOCOL_ERROR;
    Stz* szMsg = "Error: %s; %s; Message QueryExecuteRequest field maxRowsToReturn cannot be less than -1. Current value: %lld"_Stz;
    vSerialiseErrorResponse(eErrorCode,
                            szMsg,
                            aEC2[eErrorCode],
                            aErrorMsg[eErrorCode],
                            pMsg->rowsPerRowDataResponse);
    vDisconnectClient(eErrorCode);
    vNextState(READY);
    return;
  }

  vTraceComponent(TC_WLP, "Client %s: QueryExecuteRequest contains query string: %s\n"_Stz, m_sClientAddr.szStz(), pMsg->query.c_str());

  vNextState(QUERY_BEGUN);

  // Move the current QueryExecuteRequest to the QueryContext object
  // so that we can keep it until the query is completed.
  m_oQueryCtx.vInit(pMsg);
  m_pCurrentMsg = nullptr;
}

/*----------------------------------------------------------------------------*/
/* Processes a QueryProgressRequest message                                   */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vProcessQueryProgressRequest()
{
  ASSERT(m_pCurrentMsg);
  ASSERT(m_iCurrentMsgType == MessageTypes::type::QueryProgressRequest);

  //TODO: if (!bCheckState({CONNECTED})) { return; }

  QueryProgressRequest* pMsg = dynamic_cast<QueryProgressRequest*>(m_pCurrentMsg);
  if (!pMsg) {
    vErrorDynamicCast(to_string(MessageTypes::type::QueryProgressRequest).c_str());
    return;
  }

  vErrorMessageNotSupported();

  // TODO: populate

//  vNextState(DISCONNECTED);
}

/*----------------------------------------------------------------------------*/
/* Processes a SessionMonitoringRequest message                               */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vProcessSessionMonitoringRequest()
{
  ASSERT(m_pCurrentMsg);
  ASSERT(m_iCurrentMsgType == MessageTypes::type::SessionMonitoringRequest);

  //TODO: if (!bCheckState({CONNECTED})) { return; }

  SessionMonitoringRequest* pMsg = dynamic_cast<SessionMonitoringRequest*>(m_pCurrentMsg);
  if (!pMsg) {
    vErrorDynamicCast(to_string(MessageTypes::type::SessionMonitoringRequest).c_str());
    return;
  }

  vErrorMessageNotSupported();

  // TODO: populate

//  vNextState(DISCONNECTED);
}

/*----------------------------------------------------------------------------*/
/* Processes a ParseStatementRequest message                                  */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vProcessParseStatementRequest()
{
  ASSERT(m_pCurrentMsg);
  ASSERT(m_iCurrentMsgType == MessageTypes::type::ParseStatementRequest);

  //TODO: if (!bCheckState({CONNECTED})) { return; }

  ParseStatementRequest* pMsg = dynamic_cast<ParseStatementRequest*>(m_pCurrentMsg);
  if (!pMsg) {
    vErrorDynamicCast(to_string(MessageTypes::type::ParseStatementRequest).c_str());
    return;
  }

  vErrorMessageNotSupported();

  // TODO: populate

//  vNextState(DISCONNECTED);
}

/*----------------------------------------------------------------------------*/
/* Processes a StatementBindAndExecuteRequest message                         */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vProcessStatementBindAndExecuteRequest()
{
  ASSERT(m_pCurrentMsg);
  ASSERT(m_iCurrentMsgType == MessageTypes::type::StatementBindAndExecuteRequest);

  //TODO: if (!bCheckState({CONNECTED})) { return; }

  StatementBindAndExecuteRequest* pMsg = dynamic_cast<StatementBindAndExecuteRequest*>(m_pCurrentMsg);
  if (!pMsg) {
    vErrorDynamicCast(to_string(MessageTypes::type::StatementBindAndExecuteRequest).c_str());
    return;
  }

  vErrorMessageNotSupported();

  // TODO: populate

//  vNextState(DISCONNECTED);
}

/*----------------------------------------------------------------------------*/
/* Processes a ClosePreparedStatementRequest message                          */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vProcessClosePreparedStatementRequest()
{
  ASSERT(m_pCurrentMsg);
  ASSERT(m_iCurrentMsgType == MessageTypes::type::ClosePreparedStatementRequest);

  //TODO: if (!bCheckState({CONNECTED})) { return; }

  ClosePreparedStatementRequest* pMsg = dynamic_cast<ClosePreparedStatementRequest*>(m_pCurrentMsg);
  if (!pMsg) {
    vErrorDynamicCast(to_string(MessageTypes::type::ClosePreparedStatementRequest).c_str());
    return;
  }

  vErrorMessageNotSupported();

  // TODO: populate

//  vNextState(DISCONNECTED);
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -*/


/*----------------------------------------------------------------------------*/
/* Check that we are in the correct state when receiving the current message  */
/* Returns true if everything is ok, or false if we cannot continue.          */
/*----------------------------------------------------------------------------*/
bool AdaptorWL::bCheckState(std::initializer_list<eState> eProtocolStates)
{
  if (m_eProtocolState == DISCONNECTED) {
    // Can't really do anything here..the client is already disconnected
    return false;
  }

  for (eState eProtocolState : eProtocolStates) {
    if (m_eProtocolState == eProtocolState) {
      return true;
    }
  }

  // Is it possible to send an error message to the client?
  bool bCanSendError = false;

  switch (m_eProtocolState) {
    case PROTOCOL_COMPATIBLE:       // fall through
    case TUNNELED:                  // fall through
    case KERBEROS_NEGOTIATING:      // fall through
    case AUTHENTICATED:             // fall through
    case READY:                     // fall through
    case QUERY_BEGUN:               // fall through
    case API_STATEMENT_TO_SUBMIT:   // fall through
    case API_STATEMENT_SUBMITTED:   // fall through
    case API_METADATA_RECEIVED:     // fall through
    case API_DATA_RECEIVED:         // fall through
    case API_STATEMENT_COMPLETED:   // fall through
    case API_CHILD_JOB_EXITED:
      bCanSendError = true;
      break;

    default:
      // Not possible
      bCanSendError = false;
      break;
  }

  const eEC eErrType = EC_WL_PROTOCOL_ERROR;

  if (bCanSendError) {
    // Make a string list of all applicable states for the error message.
    s2K sBuf;
    bool bFirst = true;
    for (eState eProtocolState: eProtocolStates) {
      if (bFirst) {
        bFirst = false;
      } else {
        sBuf.vConcat(", "_Stz);
      }

      sBuf.vConcat(aState[eProtocolState]);
    }

    Stz* szErrMsg = "Error: %s; %s; Current protocol state is %s. Expected to be in one of the following states [%s]"_Stz;

    vSerialiseErrorResponse(eErrType,
                            szErrMsg,
                            aEC2[eErrType],
                            aErrorMsg[eErrType],
                            aState[m_eProtocolState],
                            sBuf.szStz());
  }

  // Disconnect the client. This will get picked up in OP_AdaptorWL and
  // will cause it to emit an error marker (EV_M_ERROR).
  vDisconnectClient(eErrType);

  return false;
}

/*----------------------------------------------------------------------------*/
/* Internal error doing dynamic cast of received wireline message             */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vErrorDynamicCast(const char* pcTargetType)
{
  Stz* szErrMsg = "Error: %s; %s; Unable to cast decoded message to type %s"_Stz;
  const eEC eErrType = EC_WL_INTERNAL_ERROR;

  vSerialiseErrorResponse(eErrType,
                          szErrMsg,
                          aEC2[eErrType],
                          aErrorMsg[eErrType],
                          pcTargetType);
}

/*----------------------------------------------------------------------------*/
/* Internal error doing protocol state transition                             */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vErrorStateTransition(Stz* szFromState, Stz* szToState)
{
  Stz* szErrMsg = "Error: %s; %s; Invalid state transition from state %s to state %s"_Stz;
  const eEC eErrType = EC_WL_PROTOCOL_ERROR;

  vSerialiseErrorResponse(eErrType,
                          szErrMsg,
                          aEC2[eErrType],
                          aErrorMsg[eErrType],
                          szFromState,
                          szToState);
}

/*----------------------------------------------------------------------------*/
/* Wireline message is not supported yet                                      */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vErrorMessageNotSupported()
{
  Stz* szErrMsg = "Error: %s; %s; %s"_Stz;
  const eEC eErrType = EC_WL_MSG_NOT_SUPPORTED;

  vSerialiseErrorResponse(eErrType,
                          szErrMsg,
                          aEC2[eErrType],
                          aErrorMsg[eErrType],
                          to_string((MessageTypes::type)m_iCurrentMsgType).c_str());
}

/*----------------------------------------------------------------------------*/
/* Wireline feature is not supported yet                                      */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vErrorFeatureNotSupported(Stz* szFeature)
{
  Stz* szErrMsg = "Error: %s; %s; %s"_Stz;
  const eEC eErrType = EC_WL_FEATURE_NOT_SUPPORTED;

  vSerialiseErrorResponse(eErrType,
                          szErrMsg,
                          aEC2[eErrType],
                          aErrorMsg[eErrType],
                          szFeature);
}

/*----------------------------------------------------------------------------*/
/* Disconnect the client socket to be closed                                  */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vDisconnectClient(eEC eReason)
{
  // Setting m_eProtoccolError to a value that isn't EC_NONE will cause
  // the socket to be closed. This happens because the send code ought to
  // be constantly checking this after each operation (as well as for any
  // serialised messages to send). If this value is not EC_NONE then an
  // error is passed down to the OP_SendWL which will then close the socket.

  m_eProtocolError = eReason;
  m_eProtocolState = DISCONNECTED;
  vCleanupCurrentMsg();
  m_oQueryCtx.vAbortEarly(true);
}

/*----------------------------------------------------------------------------*/
/* Returns true if a query is currently being executed                        */
/*----------------------------------------------------------------------------*/
bool AdaptorWL::bQueryInProgress()
{
  switch (m_eProtocolState) {
    case QUERY_BEGUN:
    case API_STATEMENT_SUBMITTED:
    case API_METADATA_RECEIVED:
    case API_DATA_RECEIVED:
    case API_STATEMENT_COMPLETED:
      return true;

    default:
      return false;
  }
}


/*----------------------------------------------------------------------------*/
/* Convert a TypeId to a wireline data type (DataType::type)                  */
/* We return as an i8 because we can't forward declare this enum in the hpp.  */
/*----------------------------------------------------------------------------*/
i8 AdaptorWL::xIdToWLType(TypeId xId)
{
  eCT eColType = xId.eCTVal();
  eIV eIvlType;

  switch (eColType) {
    case CT_NONE:         return -1;
    case CT_BOOLEAN:      return DataType::BOOLEAN;
    case CT_CHAR:         return DataType::VARCHAR;
    case CT_DATE:         return DataType::DATE;
    case CT_DECIMAL:      return DataType::DECIMAL;
    case CT_FLOAT:        return DataType::FLOAT;
    case CT_INTEGER:      return DataType::BIGINT;

    case CT_INTERVAL_DT:  // falls through
    case CT_INTERVAL_YM:
      eIvlType = xId.eIVVal();

      switch (eIvlType) {
        case IV_NONE:                         NEVER_REACH
        case IV_SUBCATEGORY_YEARMONTH_START:  NEVER_REACH
        case IV_YEAR:               return DataType::INTERVAL_YEAR;
        case IV_MONTH:              return DataType::INTERVAL_MONTH;
        case IV_YEAR_TO_MONTH:      return DataType::INTERVAL_YEAR_TO_MONTH;
        case IV_SUBCATEGORY_YEARMONTH_END:    NEVER_REACH
        case IV_SUBCATEGORY_DAYTIME_START:    NEVER_REACH
        case IV_DAY:                return DataType::INTERVAL_DAY;
        case IV_HOUR:               return DataType::INTERVAL_HOUR;
        case IV_MINUTE:             return DataType::INTERVAL_MINUTE;
        case IV_SECOND:             return DataType::INTERVAL_SECOND;
        case IV_DAY_TO_HOUR:        return DataType::INTERVAL_DAY_TO_HOUR;
        case IV_DAY_TO_MINUTE:      return DataType::INTERVAL_DAY_TO_MINUTE;
        case IV_DAY_TO_SECOND:      return DataType::INTERVAL_DAY_TO_SECOND;
        case IV_HOUR_TO_MINUTE:     return DataType::INTERVAL_HOUR_TO_MINUTE;
        case IV_HOUR_TO_SECOND:     return DataType::INTERVAL_HOUR_TO_SECOND;
        case IV_MINUTE_TO_SECOND:   return DataType::INTERVAL_MINUTE_TO_SECOND;
        case IV_SUBCATEGORY_DAYTIME_END:    NEVER_REACH
        case IV_ENUMS:                      NEVER_REACH
        default:                            NEVER_REACH
      }
      NEVER_REACH

    case CT_TIME:         return DataType::TIME;
    case CT_TIMESTAMP:    return DataType::TIMESTAMP;
    case CT_VARCHAR:      return DataType::VARCHAR;
    case CT_UNKNOWN:      return DataType::NULL;
    case CT_ENUMS:        return -1;
    default:              NEVER_REACH
  }
}

/*----------------------------------------------------------------------------*/
/* Work out how many rows to send in the RowDataResponse                      */
/*----------------------------------------------------------------------------*/
i8 AdaptorWL::nRowsToSend(i8 iRowsInBuf)
{
  i8 iMaxRows         = m_oQueryCtx.iMaxRowsToReturn();
  i8 iRowsAlreadySent = m_pStmtCtx->nAffectedRows();
  i8 iRowsPerResponse = m_oQueryCtx.iRowsPerRowDataResponse();
  i8 iRowsToSend      = iRowsInBuf;

  // Work out how many rows to send in this RowDataResponse
  if (iMaxRows != -1) {
    iRowsToSend = iMaxRows - iRowsAlreadySent;

    if (iRowsToSend > iRowsInBuf) {
      iRowsToSend = iRowsInBuf;
    }
  }

  if (iRowsPerResponse > 0 && iRowsToSend > iRowsPerResponse) {
    iRowsToSend = iRowsPerResponse;
  }

  return iRowsToSend;
}

/*----------------------------------------------------------------------------*/
/* Work out how many rows to send in the RowDataResponse                      */
/*----------------------------------------------------------------------------*/
void AdaptorWL::vCreateAndAddRow(std::vector<com::rapidsdata::wirelineprotocol::messages::v5::Row>* pRowList,
                                 Row* pRow,
                                 StreamDef* pStreamDef)
{
  typedef com::rapidsdata::wirelineprotocol::messages::v5::Row RowWL;
  typedef com::rapidsdata::wirelineprotocol::messages::v5::ColumnValue ColumnWL;
  typedef com::rapidsdata::wirelineprotocol::messages::v5::Date DateWL;
  typedef com::rapidsdata::wirelineprotocol::messages::v5::Time TimeWL;
  typedef com::rapidsdata::wirelineprotocol::messages::v5::Timestamp TimestampWL;
  typedef com::rapidsdata::wirelineprotocol::messages::v5::Interval IntervalWL;

  RowWL xRowWL;
  std::vector<ColumnWL> xColumns;

  for (i8 i_Col = 0;
       i_Col < pStreamDef->nColumns();
       i_Col++) {

    ColumnWL    xColumnWL;
    DateWL      xDateWL;
    TimeWL      xTimeWL;
    TimestampWL xTimestampWL;
    IntervalWL  xIntervalWL;
    Dtm         wDtm;
    Bcd         xBcd;
    SP_Bcd      spBcd;
    d8*         pDbl;

    Column* pColumn = pStreamDef->pGetColumn(i_Col);
    eDM eDatum = pColumn->eDatum();

    // Null check with special case for strings and decimals (why?)
    bool bIsNull = false;
    switch (eDatum) {
      case DM_VCH:
      case DM_BCD:
        if (pRow->bIsColNull(i_Col)) {
          bIsNull = true;
        }
        break;

      default:
        if (pRow->iColVal_int(i_Col) == C_DATUM_NULL) {
          bIsNull = true;
        }
        break;
    }

    if (bIsNull) {
      xColumnWL.__set_isNull(true);
      xColumns.push_back(xColumnWL);
      continue;
    }

    // Not null value
    i8 iVal = pRow->iColVal_int(i_Col);

    switch (eDatum) {
      case DM_BCD:
        xBcd = iVal;
        xColumnWL.__set_bigDecimalVal(spBcd.sz(xBcd.T.m_iMantissa,xBcd.T.m_iScale)->azChar()); // Fix
        break;

      case DM_BOL:
        xColumnWL.__set_booleanVal(iVal);
        break;

      case DM_DTE:
        wDtm = iVal;
        xDateWL.__set_year(wDtm.m_wDte.iYear());
        xDateWL.__set_month(wDtm.m_wDte.iMonth());
        xDateWL.__set_day(wDtm.m_wDte.iDay());
        xColumnWL.__set_dateVal(xDateWL);
        break;

      case DM_FLT:
        pDbl = (d8*) &iVal;
        xColumnWL.__set_doubleVal(*pDbl);
        break;

      case DM_INT:
        xColumnWL.__set_bigintVal(iVal);
        break;

      case DM_IVD:  // day-time, falls through
      case DM_IVY:  // year-month
        // We need to set the specific interval type and then only set the fields that matter.
        wDtm = iVal;

        xIntervalWL.__set_isNegative(wDtm.m_wIvl.T.m_xIvl.m_bIsNeg);

        switch (wDtm.m_wIvl.T.m_xIvl.m_eType) {
          case IV_NONE:                         NEVER_REACH
          case IV_SUBCATEGORY_YEARMONTH_START:  NEVER_REACH

          case IV_YEAR:
            xIntervalWL.__set_type(IntervalType::type::YEAR);
            xIntervalWL.__set_year((i8) wDtm.m_wIvl.T.m_xIvl.u.m_xIvlYM.m_uMonths / 12);
            xIntervalWL.__set_month((i8) 0);
            break;

          case IV_MONTH:
            xIntervalWL.__set_type(IntervalType::type::MONTH);
            xIntervalWL.__set_year((i8) 0);
            xIntervalWL.__set_month((i8) wDtm.m_wIvl.T.m_xIvl.u.m_xIvlYM.m_uMonths);
            break;

          case IV_YEAR_TO_MONTH:
            xIntervalWL.__set_type(IntervalType::type::YEAR_TO_MONTH);
            xIntervalWL.__set_year((i8) wDtm.m_wIvl.T.m_xIvl.u.m_xIvlYM.m_uMonths / 12);
            xIntervalWL.__set_month((i8) wDtm.m_wIvl.T.m_xIvl.u.m_xIvlYM.m_uMonths % 12);
            break;

          case IV_SUBCATEGORY_YEARMONTH_END:    NEVER_REACH
          case IV_SUBCATEGORY_DAYTIME_START:    NEVER_REACH

          case IV_DAY:
            xIntervalWL.__set_type(IntervalType::type::DAY);
            xIntervalWL.__set_day((i8) wDtm.m_wIvl.T.m_xIvl.u.m_xIvlDT.m_uMicros / (C_SECS_PER_DAY * C_1M));
            xIntervalWL.__set_hour((i8) 0);
            xIntervalWL.__set_minute((i8) 0);
            xIntervalWL.__set_second((i8) 0);
            xIntervalWL.__set_nanoseconds((i8) 0);
            break;

          case IV_HOUR:
            xIntervalWL.__set_type(IntervalType::type::HOUR);
            xIntervalWL.__set_day((i8) 0);
            xIntervalWL.__set_hour((i8) wDtm.m_wIvl.T.m_xIvl.u.m_xIvlDT.m_uMicros / (C_SECS_PER_HR * C_1M));
            xIntervalWL.__set_minute((i8) 0);
            xIntervalWL.__set_second((i8) 0);
            xIntervalWL.__set_nanoseconds((i8) 0);
            break;

          case IV_MINUTE:
            xIntervalWL.__set_type(IntervalType::type::MINUTE);
            xIntervalWL.__set_day((i8) 0);
            xIntervalWL.__set_hour((i8) 0);
            xIntervalWL.__set_minute((i8) wDtm.m_wIvl.T.m_xIvl.u.m_xIvlDT.m_uMicros / (C_SECS_PER_MIN * C_1M));
            xIntervalWL.__set_second((i8) 0);
            xIntervalWL.__set_nanoseconds((i8) 0);
            break;

          case IV_SECOND:
            xIntervalWL.__set_type(IntervalType::type::SECOND);
            xIntervalWL.__set_day((i8) 0);
            xIntervalWL.__set_hour((i8) 0);
            xIntervalWL.__set_minute((i8) 0);
            xIntervalWL.__set_second((i8) wDtm.m_wIvl.T.m_xIvl.u.m_xIvlDT.m_uMicros / C_1M);
            xIntervalWL.__set_nanoseconds((i8) (wDtm.m_wIvl.T.m_xIvl.u.m_xIvlDT.m_uMicros % C_1M) * C_1K);
            break;

          case IV_DAY_TO_HOUR:
            xIntervalWL.__set_type(IntervalType::type::DAY_TO_HOUR);
            xIntervalWL.__set_day((i8) wDtm.m_wIvl.T.m_xIvl.u.m_xIvlDT.m_uMicros    / (C_SECS_PER_DAY * C_1M));
            xIntervalWL.__set_hour((i8) wDtm.m_wIvl.T.m_xIvl.u.m_xIvlDT.m_uMicros   % (C_SECS_PER_DAY * C_1M) / (C_SECS_PER_HR * C_1M));
            xIntervalWL.__set_minute((i8) 0);
            xIntervalWL.__set_second((i8) 0);
            xIntervalWL.__set_nanoseconds((i8) 0);
            break;

          case IV_DAY_TO_MINUTE:
            xIntervalWL.__set_type(IntervalType::type::DAY_TO_MINUTE);
            xIntervalWL.__set_day((i8) wDtm.m_wIvl.T.m_xIvl.u.m_xIvlDT.m_uMicros    / (C_SECS_PER_DAY * C_1M));
            xIntervalWL.__set_hour((i8) wDtm.m_wIvl.T.m_xIvl.u.m_xIvlDT.m_uMicros   % (C_SECS_PER_DAY * C_1M) / (C_SECS_PER_HR * C_1M));
            xIntervalWL.__set_minute((i8) wDtm.m_wIvl.T.m_xIvl.u.m_xIvlDT.m_uMicros % (C_SECS_PER_HR * C_1M)  / (C_SECS_PER_MIN * C_1M));
            xIntervalWL.__set_second((i8) 0);
            xIntervalWL.__set_nanoseconds((i8) 0);
            break;

          case IV_DAY_TO_SECOND:
            xIntervalWL.__set_type(IntervalType::type::DAY_TO_SECOND);
            xIntervalWL.__set_day((i8) wDtm.m_wIvl.T.m_xIvl.u.m_xIvlDT.m_uMicros    / (C_SECS_PER_DAY * C_1M));
            xIntervalWL.__set_hour((i8) wDtm.m_wIvl.T.m_xIvl.u.m_xIvlDT.m_uMicros   % (C_SECS_PER_DAY * C_1M) / (C_SECS_PER_HR * C_1M));
            xIntervalWL.__set_minute((i8) wDtm.m_wIvl.T.m_xIvl.u.m_xIvlDT.m_uMicros % (C_SECS_PER_HR * C_1M)  / (C_SECS_PER_MIN * C_1M));
            xIntervalWL.__set_second((i8) wDtm.m_wIvl.T.m_xIvl.u.m_xIvlDT.m_uMicros % (C_SECS_PER_MIN * C_1M) / C_1M);
            xIntervalWL.__set_nanoseconds((i8) (wDtm.m_wIvl.T.m_xIvl.u.m_xIvlDT.m_uMicros % C_1M) * C_1K);
            break;

          case IV_HOUR_TO_MINUTE:
            xIntervalWL.__set_type(IntervalType::type::HOUR_TO_MINUTE);
            xIntervalWL.__set_day((i8) 0);
            xIntervalWL.__set_hour((i8) wDtm.m_wIvl.T.m_xIvl.u.m_xIvlDT.m_uMicros   / (C_SECS_PER_HR * C_1M));
            xIntervalWL.__set_minute((i8) wDtm.m_wIvl.T.m_xIvl.u.m_xIvlDT.m_uMicros % (C_SECS_PER_HR * C_1M)  / (C_SECS_PER_MIN * C_1M));
            xIntervalWL.__set_second((i8) 0);
            xIntervalWL.__set_nanoseconds((i8) 0);
            break;

          case IV_HOUR_TO_SECOND:
            xIntervalWL.__set_type(IntervalType::type::HOUR_TO_SECOND);
            xIntervalWL.__set_day((i8) 0);
            xIntervalWL.__set_hour((i8) wDtm.m_wIvl.T.m_xIvl.u.m_xIvlDT.m_uMicros   / (C_SECS_PER_HR * C_1M));
            xIntervalWL.__set_minute((i8) wDtm.m_wIvl.T.m_xIvl.u.m_xIvlDT.m_uMicros % (C_SECS_PER_HR * C_1M)  / (C_SECS_PER_MIN * C_1M));
            xIntervalWL.__set_second((i8) wDtm.m_wIvl.T.m_xIvl.u.m_xIvlDT.m_uMicros % (C_SECS_PER_MIN * C_1M) / C_1M);
            xIntervalWL.__set_nanoseconds((i8) (wDtm.m_wIvl.T.m_xIvl.u.m_xIvlDT.m_uMicros % C_1M) * C_1K);
            break;

          case IV_MINUTE_TO_SECOND:
            xIntervalWL.__set_type(IntervalType::type::MINUTE_TO_SECOND);
            xIntervalWL.__set_day((i8) 0);
            xIntervalWL.__set_hour((i8) 0);
            xIntervalWL.__set_minute((i8) wDtm.m_wIvl.T.m_xIvl.u.m_xIvlDT.m_uMicros / (C_SECS_PER_MIN * C_1M));
            xIntervalWL.__set_second((i8) wDtm.m_wIvl.T.m_xIvl.u.m_xIvlDT.m_uMicros % (C_SECS_PER_MIN * C_1M) / C_1M);
            xIntervalWL.__set_nanoseconds((i8) (wDtm.m_wIvl.T.m_xIvl.u.m_xIvlDT.m_uMicros % C_1M) * C_1K);
            break;

          case IV_SUBCATEGORY_DAYTIME_END:      NEVER_REACH
          case IV_ENUMS:                        NEVER_REACH
        }

        xColumnWL.__set_intervalVal(xIntervalWL);
        break;

      case DM_TME:
        wDtm = iVal;
        xTimeWL.__set_hour((i2) wDtm.m_wTme.iHour());
        xTimeWL.__set_minute((i2) wDtm.m_wTme.iMin());
        xTimeWL.__set_second((i2) wDtm.m_wTme.iSec());
        xTimeWL.__set_nanoseconds((i4) (wDtm.m_wTme.m_uMicro * (u8) C_1K));
        xColumnWL.__set_timeVal(xTimeWL);
        break;

      case DM_TSP:
        wDtm = iVal;
        xTimestampWL.__set_year((i4) wDtm.m_wTsp.iYear());
        xTimestampWL.__set_month((i2) wDtm.m_wTsp.iMonth());
        xTimestampWL.__set_day((i2) wDtm.m_wTsp.iDay());
        xTimestampWL.__set_hour((i2) wDtm.m_wTsp.iHour());
        xTimestampWL.__set_minute((i2) wDtm.m_wTsp.iMin());
        xTimestampWL.__set_second((i2) wDtm.m_wTsp.iSec());
        xTimestampWL.__set_nanoseconds((i4) (wDtm.m_wTsp.iMicro() * C_1K));
        xColumnWL.__set_timestampVal(xTimestampWL);
        break;

      case DM_VCH:
        xColumnWL.__set_stringVal(pRow->szColVal_ofs(i_Col)->azChar());
        break;

      case DM_UNK:
        xColumnWL.__set_isNull(true);
        break;

      default:
        NEVER_REACH
    }

    xColumns.push_back(xColumnWL);
  }

  // Add the columns to the row, and the row to the rowlist
  xRowWL.__set_columns(xColumns);
  pRowList->push_back(xRowWL);
}



/*----------------------------------------------------------------------------*/
/* Helper to convert an internal AffectedVerb enum to the wireline version.   */
/*----------------------------------------------------------------------------*/
com::rapidsdata::wirelineprotocol::messages::v5::AffectedVerb::type
eToWLAffectedVerb(WLP::StatementContext::eAffectedVerb eVerb) {

  switch (eVerb) {
    case WLP::StatementContext::NONE:      return AffectedVerb::type::NONE;
    case WLP::StatementContext::SELECTED:  return AffectedVerb::type::SELECTED;
    case WLP::StatementContext::INSERTED:  return AffectedVerb::type::INSERTED;
    case WLP::StatementContext::UPDATED:   return AffectedVerb::type::UPDATED;
    case WLP::StatementContext::DELETED:   return AffectedVerb::type::DELETED;
    default: NEVER_REACH;
  }
}

/*============================================================================*/
/* end                                                                        */
/*============================================================================*/
