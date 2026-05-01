use std::marker::PhantomData;

use crate::{AliasedExpr, Expr, Field, NotNull, Nullable};

/// Maps SQL nullability markers to Rust selected value types.
pub trait NullabilityOutput<T> {
    type Output;
}

impl<T> NullabilityOutput<T> for NotNull {
    type Output = T;
}

impl<T> NullabilityOutput<T> for Nullable {
    type Output = Option<T>;
}

/// A selected expression in a query, optionally with a SQL column alias.
#[derive(Debug, Clone)]
pub struct SelectItem {
    pub(crate) expr: crate::ExprNode,
    pub(crate) alias: Option<&'static str>,
}

/// Marker used for selections that are not a single SQL expression.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub enum NotSingleColumn {}

/// Something that can appear in a `select(...)` or `returning(...)` list.
pub trait Selectable {
    type Record;
    #[doc(hidden)]
    type SingleSql;

    fn into_select_items(self) -> Vec<SelectItem>;
}

impl<T, N> Selectable for Field<T, N>
where
    N: NullabilityOutput<T>,
{
    type Record = <N as NullabilityOutput<T>>::Output;
    type SingleSql = T;

    fn into_select_items(self) -> Vec<SelectItem> {
        vec![SelectItem {
            expr: self.expr().into_node(),
            alias: None,
        }]
    }
}

impl<T, N> Selectable for Expr<T, N>
where
    N: NullabilityOutput<T>,
{
    type Record = <N as NullabilityOutput<T>>::Output;
    type SingleSql = T;

    fn into_select_items(self) -> Vec<SelectItem> {
        vec![SelectItem {
            expr: self.into_node(),
            alias: None,
        }]
    }
}

impl<T, N> Selectable for AliasedExpr<T, N>
where
    N: NullabilityOutput<T>,
{
    type Record = <N as NullabilityOutput<T>>::Output;
    type SingleSql = T;

    fn into_select_items(self) -> Vec<SelectItem> {
        vec![SelectItem {
            expr: self.expr,
            alias: Some(self.alias),
        }]
    }
}

/// Override the Rust record type associated with an existing selection.
#[derive(Debug, Clone)]
pub struct SelectionAs<R, S> {
    selection: S,
    marker: PhantomData<fn() -> R>,
}

/// Override a selection's associated `Record` type.
pub fn selection_as<R, S>(selection: S) -> SelectionAs<R, S> {
    SelectionAs {
        selection,
        marker: PhantomData,
    }
}

impl<R, S> Selectable for SelectionAs<R, S>
where
    S: Selectable,
{
    type Record = R;
    type SingleSql = S::SingleSql;

    fn into_select_items(self) -> Vec<SelectItem> {
        self.selection.into_select_items()
    }
}

macro_rules! impl_single_tuple_selectable {
    ($ty:ident $var:ident) => {
        impl<$ty> Selectable for ($ty,)
        where
            $ty: Selectable,
        {
            type Record = ($ty::Record,);
            type SingleSql = $ty::SingleSql;

            fn into_select_items(self) -> Vec<SelectItem> {
                let ($var,) = self;
                $var.into_select_items()
            }
        }
    };
}

macro_rules! impl_multi_tuple_selectable {
    ($($ty:ident $var:ident),+ $(,)?) => {
        impl<$($ty),+> Selectable for ($($ty,)+)
        where
            $($ty: Selectable),+
        {
            type Record = ($($ty::Record,)+);
            type SingleSql = NotSingleColumn;

            fn into_select_items(self) -> Vec<SelectItem> {
                let ($($var,)+) = self;
                let mut items = Vec::new();
                $(
                    items.extend($var.into_select_items());
                )+
                items
            }
        }
    };
}

impl_single_tuple_selectable!(A a);
impl_multi_tuple_selectable!(A a, B b);
impl_multi_tuple_selectable!(A a, B b, C c);
impl_multi_tuple_selectable!(A a, B b, C c, D d);
impl_multi_tuple_selectable!(A a, B b, C c, D d, E e);
impl_multi_tuple_selectable!(A a, B b, C c, D d, E e, F f);
impl_multi_tuple_selectable!(A a, B b, C c, D d, E e, F f, G g);
impl_multi_tuple_selectable!(A a, B b, C c, D d, E e, F f, G g, H h);
impl_multi_tuple_selectable!(A a, B b, C c, D d, E e, F f, G g, H h, I i);
impl_multi_tuple_selectable!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j);
impl_multi_tuple_selectable!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k);
impl_multi_tuple_selectable!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l);
impl_multi_tuple_selectable!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l, M m);
impl_multi_tuple_selectable!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l, M m, N n);
impl_multi_tuple_selectable!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l, M m, N n, O o);
impl_multi_tuple_selectable!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l, M m, N n, O o, P p);
