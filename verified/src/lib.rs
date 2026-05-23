use vstd::prelude::*;

fn main() {}

verus! {

pub enum NodeKind {
    Node4,
    Node16,
    Node48,
    Node256,
}

pub enum PackedShape {
    Empty,
    UnaryPrefix,
    Inner(NodeKind),
}

pub open spec fn capacity(kind: NodeKind) -> nat {
    match kind {
        NodeKind::Node4 => 4,
        NodeKind::Node16 => 16,
        NodeKind::Node48 => 48,
        NodeKind::Node256 => 256,
    }
}

pub open spec fn packed_shape_for_len(len: nat) -> PackedShape {
    if len == 0 {
        PackedShape::Empty
    } else if len == 1 {
        PackedShape::UnaryPrefix
    } else if len <= 4 {
        PackedShape::Inner(NodeKind::Node4)
    } else if len <= 16 {
        PackedShape::Inner(NodeKind::Node16)
    } else if len <= 48 {
        PackedShape::Inner(NodeKind::Node48)
    } else {
        PackedShape::Inner(NodeKind::Node256)
    }
}

pub proof fn lemma_packed_shape_fits_live_child_count(len: nat)
    requires
        len <= 256,
    ensures
        match packed_shape_for_len(len) {
            PackedShape::Empty => len == 0,
            PackedShape::UnaryPrefix => len == 1,
            PackedShape::Inner(kind) => 2 <= len && len <= capacity(kind),
        },
{
}

pub open spec fn sorted_unique(keys: Seq<u8>) -> bool {
    forall|i: int, j: int| 0 <= i < j < keys.len() ==> keys[i] < keys[j]
}

pub open spec fn contains_key(keys: Seq<u8>, byte: u8) -> bool
    decreases keys.len()
{
    if keys.len() == 0 {
        false
    } else {
        keys[0] == byte || contains_key(keys.drop_first(), byte)
    }
}

pub open spec fn find_index(keys: Seq<u8>, byte: u8) -> Option<int>
    decreases keys.len()
{
    if keys.len() == 0 {
        None
    } else if keys[0] == byte {
        Some(0)
    } else {
        match find_index(keys.drop_first(), byte) {
            Some(i) => Some(i + 1),
            None => None,
        }
    }
}

pub proof fn lemma_find_index_in_bounds(keys: Seq<u8>, byte: u8)
    ensures
        match find_index(keys, byte) {
            Some(i) => 0 <= i < keys.len(),
            None => true,
        },
    decreases keys.len()
{
    if keys.len() == 0 {
    } else if keys[0] == byte {
    } else {
        lemma_find_index_in_bounds(keys.drop_first(), byte);
    }
}

pub ghost struct InnerNode {
    pub kind: NodeKind,
    pub keys: Seq<u8>,
    pub children: Seq<nat>,
}

impl InnerNode {
    pub open spec fn wf(self) -> bool {
        self.keys.len() == self.children.len()
            && self.keys.len() <= capacity(self.kind)
            && sorted_unique(self.keys)
            && forall|i: int| 0 <= i < self.children.len() ==> self.children[i] > 0
    }

    pub open spec fn child_at(self, byte: u8) -> Option<nat> {
        match find_index(self.keys, byte) {
            Some(i) => Some(self.children[i]),
            None => None,
        }
    }
}

pub proof fn lemma_child_at_hit_is_live(node: InnerNode, byte: u8)
    requires
        node.wf(),
    ensures
        match node.child_at(byte) {
            Some(child) => child > 0,
            None => true,
        },
{
    lemma_find_index_in_bounds(node.keys, byte);
}

pub open spec fn insert_absent_keys(keys: Seq<u8>, byte: u8) -> Seq<u8>
    decreases keys.len()
{
    if keys.len() == 0 {
        seq![byte]
    } else if byte < keys[0] {
        seq![byte].add(keys)
    } else {
        seq![keys[0]].add(insert_absent_keys(keys.drop_first(), byte))
    }
}

pub open spec fn insert_absent_children(
    keys: Seq<u8>,
    children: Seq<nat>,
    byte: u8,
    child: nat,
) -> Seq<nat>
    decreases keys.len()
{
    if keys.len() == 0 {
        seq![child]
    } else if byte < keys[0] {
        seq![child].add(children)
    } else {
        seq![children[0]].add(insert_absent_children(keys.drop_first(), children.drop_first(), byte, child))
    }
}

pub proof fn lemma_insert_absent_keys_len(keys: Seq<u8>, byte: u8)
    requires
        !contains_key(keys, byte),
    ensures
        insert_absent_keys(keys, byte).len() == keys.len() + 1,
    decreases keys.len()
{
    if keys.len() == 0 {
    } else if byte < keys[0] {
    } else {
        lemma_insert_absent_keys_len(keys.drop_first(), byte);
    }
}

pub proof fn lemma_insert_absent_children_len(
    keys: Seq<u8>,
    children: Seq<nat>,
    byte: u8,
    child: nat,
)
    requires
        keys.len() == children.len(),
        !contains_key(keys, byte),
    ensures
        insert_absent_children(keys, children, byte, child).len() == children.len() + 1,
    decreases keys.len()
{
    if keys.len() == 0 {
    } else if byte < keys[0] {
    } else {
        lemma_insert_absent_children_len(keys.drop_first(), children.drop_first(), byte, child);
    }
}

pub proof fn lemma_insert_absent_preserves_arity(
    keys: Seq<u8>,
    children: Seq<nat>,
    byte: u8,
    child: nat,
)
    requires
        keys.len() == children.len(),
        !contains_key(keys, byte),
    ensures
        insert_absent_keys(keys, byte).len()
            == insert_absent_children(keys, children, byte, child).len(),
    decreases keys.len()
{
    lemma_insert_absent_keys_len(keys, byte);
    lemma_insert_absent_children_len(keys, children, byte, child);
}

pub open spec fn grow_kind_for_len(kind: NodeKind, len: nat) -> NodeKind {
    if len <= capacity(kind) {
        kind
    } else if len <= 16 {
        NodeKind::Node16
    } else if len <= 48 {
        NodeKind::Node48
    } else {
        NodeKind::Node256
    }
}

pub open spec fn shrink_kind_for_len(kind: NodeKind, len: nat) -> NodeKind {
    match kind {
        NodeKind::Node4 => NodeKind::Node4,
        NodeKind::Node16 => if len <= 3 {
            NodeKind::Node4
        } else {
            NodeKind::Node16
        },
        NodeKind::Node48 => if len <= 12 {
            NodeKind::Node16
        } else {
            NodeKind::Node48
        },
        NodeKind::Node256 => if len <= 37 {
            NodeKind::Node48
        } else {
            NodeKind::Node256
        },
    }
}

pub proof fn lemma_grow_kind_has_capacity(kind: NodeKind, len: nat)
    requires
        len <= 256,
    ensures
        len <= capacity(grow_kind_for_len(kind, len)),
{
}

pub proof fn lemma_shrink_kind_has_capacity(kind: NodeKind, len: nat)
    requires
        len <= capacity(kind),
    ensures
        len <= capacity(shrink_kind_for_len(kind, len)),
{
}

pub open spec fn add_absent_child(node: InnerNode, byte: u8, child: nat) -> InnerNode {
    InnerNode {
        kind: grow_kind_for_len(node.kind, node.keys.len() + 1),
        keys: insert_absent_keys(node.keys, byte),
        children: insert_absent_children(node.keys, node.children, byte, child),
    }
}

pub proof fn lemma_add_absent_child_preserves_shape(node: InnerNode, byte: u8, child: nat)
    requires
        node.wf(),
        !contains_key(node.keys, byte),
        node.keys.len() < 256,
        child > 0,
    ensures
        add_absent_child(node, byte, child).keys.len()
            == add_absent_child(node, byte, child).children.len(),
        add_absent_child(node, byte, child).keys.len()
            <= capacity(add_absent_child(node, byte, child).kind),
{
    lemma_insert_absent_preserves_arity(node.keys, node.children, byte, child);
    lemma_insert_absent_keys_len(node.keys, byte);
    lemma_grow_kind_has_capacity(node.kind, node.keys.len() + 1);
}

pub open spec fn remove_present_keys(keys: Seq<u8>, byte: u8) -> Seq<u8>
    decreases keys.len()
{
    if keys.len() == 0 {
        seq![]
    } else if keys[0] == byte {
        keys.drop_first()
    } else {
        seq![keys[0]].add(remove_present_keys(keys.drop_first(), byte))
    }
}

pub open spec fn remove_present_children(
    keys: Seq<u8>,
    children: Seq<nat>,
    byte: u8,
) -> Seq<nat>
    decreases keys.len()
{
    if keys.len() == 0 {
        seq![]
    } else if keys[0] == byte {
        children.drop_first()
    } else {
        seq![children[0]].add(remove_present_children(keys.drop_first(), children.drop_first(), byte))
    }
}

pub proof fn lemma_remove_present_keys_len(keys: Seq<u8>, byte: u8)
    requires
        contains_key(keys, byte),
    ensures
        remove_present_keys(keys, byte).len() + 1 == keys.len(),
    decreases keys.len()
{
    if keys.len() == 0 {
    } else if keys[0] == byte {
    } else {
        lemma_remove_present_keys_len(keys.drop_first(), byte);
    }
}

pub proof fn lemma_remove_present_children_len(
    keys: Seq<u8>,
    children: Seq<nat>,
    byte: u8,
)
    requires
        keys.len() == children.len(),
        contains_key(keys, byte),
    ensures
        remove_present_children(keys, children, byte).len() + 1 == children.len(),
    decreases keys.len()
{
    if keys.len() == 0 {
    } else if keys[0] == byte {
    } else {
        lemma_remove_present_children_len(keys.drop_first(), children.drop_first(), byte);
    }
}

pub open spec fn remove_existing_child(node: InnerNode, byte: u8) -> InnerNode {
    let keys = remove_present_keys(node.keys, byte);
    InnerNode {
        kind: shrink_kind_for_len(node.kind, keys.len()),
        keys,
        children: remove_present_children(node.keys, node.children, byte),
    }
}

pub proof fn lemma_remove_existing_child_preserves_shape(node: InnerNode, byte: u8)
    requires
        node.wf(),
        contains_key(node.keys, byte),
    ensures
        remove_existing_child(node, byte).keys.len()
            == remove_existing_child(node, byte).children.len(),
        remove_existing_child(node, byte).keys.len()
            <= capacity(remove_existing_child(node, byte).kind),
{
    lemma_remove_present_keys_len(node.keys, byte);
    lemma_remove_present_children_len(node.keys, node.children, byte);
    lemma_shrink_kind_has_capacity(node.kind, remove_existing_child(node, byte).keys.len());
}

pub open spec fn two_branch_keys(left: u8, right: u8) -> Seq<u8> {
    if left < right {
        seq![left].add(seq![right])
    } else {
        seq![right].add(seq![left])
    }
}

pub open spec fn two_branch_children(
    left_byte: u8,
    left_child: nat,
    right_byte: u8,
    right_child: nat,
) -> Seq<nat> {
    if left_byte < right_byte {
        seq![left_child].add(seq![right_child])
    } else {
        seq![right_child].add(seq![left_child])
    }
}

pub ghost struct PrefixSplit {
    pub common_prefix: Seq<u8>,
    pub existing_byte: u8,
    pub existing_child: nat,
    pub new_byte: u8,
    pub new_child: nat,
}

impl PrefixSplit {
    pub open spec fn wf(self) -> bool {
        self.existing_byte != self.new_byte && self.existing_child > 0 && self.new_child > 0
    }

    pub open spec fn branch_node(self) -> InnerNode {
        InnerNode {
            kind: NodeKind::Node4,
            keys: two_branch_keys(self.existing_byte, self.new_byte),
            children: two_branch_children(
                self.existing_byte,
                self.existing_child,
                self.new_byte,
                self.new_child,
            ),
        }
    }
}

pub proof fn lemma_prefix_split_branch_node_is_well_formed(split: PrefixSplit)
    requires
        split.wf(),
    ensures
        split.branch_node().wf(),
        split.branch_node().keys.len() == 2,
        split.branch_node().children.len() == 2,
        split.branch_node().keys.len() <= capacity(NodeKind::Node4),
{
    if split.existing_byte < split.new_byte {
    } else {
    }
}

pub open spec fn user_len(raw: Seq<u8>) -> nat {
    raw.len() + 1
}

pub open spec fn user_byte_at(raw: Seq<u8>, idx: int) -> Option<u8> {
    if 0 <= idx < raw.len() {
        Some(raw[idx])
    } else if idx == raw.len() {
        Some(0)
    } else {
        None
    }
}

pub proof fn lemma_user_key_has_terminator(raw: Seq<u8>)
    ensures
        user_len(raw) == raw.len() + 1,
        user_byte_at(raw, raw.len() as int) == Some(0),
{
}

pub proof fn lemma_strict_prefix_diverges_at_terminator(prefix: Seq<u8>, next: u8)
    requires
        next != 0,
    ensures
        user_byte_at(prefix, prefix.len() as int) == Some(0),
        Some(next) != user_byte_at(prefix, prefix.len() as int),
{
}

pub open spec fn find_delimiter(rest: Seq<u8>, delimiter: u8) -> Option<nat>
    decreases rest.len()
{
    if rest.len() == 0 {
        None
    } else if rest[0] == delimiter {
        Some(0)
    } else {
        match find_delimiter(rest.drop_first(), delimiter) {
            Some(i) => Some(i + 1),
            None => None,
        }
    }
}

pub proof fn lemma_find_delimiter_in_bounds(rest: Seq<u8>, delimiter: u8)
    ensures
        match find_delimiter(rest, delimiter) {
            Some(i) => i < rest.len(),
            None => true,
        },
    decreases rest.len()
{
    if rest.len() == 0 {
    } else if rest[0] == delimiter {
    } else {
        lemma_find_delimiter_in_bounds(rest.drop_first(), delimiter);
    }
}

pub open spec fn delimiter_rollup_len(prefix_len: nat, rest: Seq<u8>, delimiter: u8) -> Option<nat> {
    match find_delimiter(rest, delimiter) {
        Some(i) => Some(prefix_len + i + 1),
        None => None,
    }
}

pub proof fn lemma_delimiter_rollup_bounds(prefix_len: nat, rest: Seq<u8>, delimiter: u8)
    ensures
        match delimiter_rollup_len(prefix_len, rest, delimiter) {
            Some(common_len) => prefix_len < common_len && common_len <= prefix_len + rest.len(),
            None => true,
        },
{
    lemma_find_delimiter_in_bounds(rest, delimiter);
}

pub open spec fn align8(raw: nat) -> nat {
    ((raw + 7) / 8) * 8
}

pub open spec fn leaf_extent_size(key_len: nat, value_len: nat) -> nat {
    align8(2 + key_len + value_len)
}

pub proof fn lemma_leaf_extent_covers_payload(key_len: nat, value_len: nat)
    ensures
        leaf_extent_size(key_len, value_len) >= 2 + key_len + value_len,
{
}

pub proof fn lemma_leaf_extent_is_8_byte_aligned(key_len: nat, value_len: nat)
    ensures
        leaf_extent_size(key_len, value_len) % 8 == 0,
{
}

} // verus!
