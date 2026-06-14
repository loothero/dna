use apibara_dna_common::{Cursor, Hash};
pub use starknet_rust::core::types::{
    BlockWithReceipts, CallType, ContractStorageDiffItem, DataAvailabilityMode,
    DeclareTransactionContent, DeclareTransactionReceipt, DeclareTransactionTrace,
    DeclareTransactionV0Content, DeclareTransactionV1Content, DeclareTransactionV2Content,
    DeclareTransactionV3Content, DeclaredClassItem, DeployAccountTransactionContent,
    DeployAccountTransactionReceipt, DeployAccountTransactionTrace,
    DeployAccountTransactionV1Content, DeployAccountTransactionV3Content, DeployTransactionContent,
    DeployTransactionReceipt, DeployedContractItem, EmittedEvent, EmittedEventWithFinality, Event,
    ExecuteInvocation, ExecutionResources, ExecutionResult, FeePayment, Felt as FieldElement,
    FunctionCall, FunctionInvocation, InvokeTransactionContent, InvokeTransactionReceipt,
    InvokeTransactionTrace, InvokeTransactionV0Content, InvokeTransactionV1Content,
    InvokeTransactionV3Content, L1DataAvailabilityMode, L1HandlerTransactionContent,
    L1HandlerTransactionReceipt, L1HandlerTransactionTrace, L2TransactionStatus,
    MaybePreConfirmedBlockWithReceipts, MaybePreConfirmedBlockWithTxHashes,
    MaybePreConfirmedStateUpdate, MsgToL1, NonceUpdate, PreConfirmedBlockWithReceipts,
    PreConfirmedStateUpdate, PriceUnit, ReceiptBlock, ReplacedClassItem, ResourceBounds,
    ResourceBoundsMapping, ResourcePrice, StateDiff, StateUpdate, StorageEntry,
    TraceBlockTransactionsResult, Transaction, TransactionContent, TransactionFinalityStatus,
    TransactionReceipt, TransactionReceiptWithBlockInfo, TransactionTrace,
    TransactionTraceWithHash, TransactionWithL2Status, TransactionWithReceipt,
};

pub trait BlockExt {
    fn is_finalized(&self) -> bool;
    fn cursor(&self) -> Option<Cursor>;
}

pub fn felt_to_hash(value: &FieldElement) -> Hash {
    let bytes = value.to_bytes_be();
    let bytes = bytes.as_ref();
    let mut out = vec![0; 32];
    let len = bytes.len().min(32);
    out[32 - len..].copy_from_slice(&bytes[bytes.len() - len..]);
    Hash(out)
}

impl BlockExt for MaybePreConfirmedBlockWithTxHashes {
    fn is_finalized(&self) -> bool {
        let MaybePreConfirmedBlockWithTxHashes::Block(block) = self else {
            return false;
        };

        block.status == starknet_rust::core::types::BlockStatus::AcceptedOnL1
    }

    fn cursor(&self) -> Option<Cursor> {
        let MaybePreConfirmedBlockWithTxHashes::Block(block) = self else {
            return None;
        };

        let number = block.block_number;
        let hash = felt_to_hash(&block.block_hash);

        Cursor::new(number, hash).into()
    }
}
