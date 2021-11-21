// Define a coproduct, e.g.
// define_coprod_enum! { Foo {A, B} }
#[macro_export]
macro_rules! define_coprod_enum {
    ($name:ident { $($member:ident),+ }) => {
        // https://github.com/dtolnay/paste
        paste! {
            // enum Foo<TA, TB> {
            //    A(TA),
            //    B(TB)
            //}

            #[derive(Clone)]
            pub enum $name < $( [<T $member>] ),* > {
              $(
                  [<$member:camel>] ([<T $member>]),
              )*
            }
        }
    }
}

// Define a coproduct impl, e.g.
// define_coprod_impl! { Foo {A, B} }
#[macro_export]
macro_rules! define_coprod_impl {
    ($name:ident { $($member:ident),+ }) => {
        paste! {
            // impl<TA, TB> Foo<TA, TB> {
            impl < $( [<T $member>] ),* > $name < $( [<T $member>] ),* > {

                // fn fold<FA, FB, R>(&self, fa: FA, fb: FB) -> R
                // where
                //     FA: Fn(&TA) -> R,
                //     FB: Fn(&TB) -> R,
                // {
                //     match self {
                //         Foo::A(x) => fa(x),
                //         Foo::B(x) => fb(x),
                //     }
                // }
                fn fold< $( [<F $member>] ),* , R>(
                    &self,
                    $( [<f $member:snake:lower>]: [<F $member>] ),*
                ) -> R
                where
                    $( [<F $member>]: Fn(&[<T $member>]) -> R),*
                {
                    match self {
                        $( $name::[<$member:camel>](x) => [<f $member:snake:lower>](x) ),*
                    }
                }

                fn fold_into< $( [<F $member>] ),* , R>(
                    self,
                    $( [<f $member:snake:lower>]: [<F $member>] ),*
                ) -> R
                where
                    $( [<F $member>]: Fn([<T $member>]) -> R),*
                {
                    match self {
                        $( $name::[<$member:camel>](x) => [<f $member:snake:lower>](x) ),*
                    }
                }


                // fn map<FA, FB, RA, RB>(&self, fa: FA, fb: FB) -> Foo<RA, RB>
                // where
                //     FA: Fn(&TA) -> RA,
                //     FB: Fn(&TB) -> RB,
                // {
                //     self.fold(|x| Foo::A(fa(x)), |x| Foo::B(fb(x)))
                // }
                fn map<$( [<F $member>] ),*, $( [<R $member>] ),*>(
                    &self,
                    $( [<f $member:snake:lower>]: [<F $member>] ),*
                ) -> $name<$( [<R $member>] ),*>
                where
                    $( [<F $member>]: Fn(&[<T $member>]) -> [<R $member>] ),*
                {
                    self.fold(
                        $( |x| $name::[<$member:camel>]([<f $member:snake:lower>](x)) ),*
                    )
                }

                fn map_into<$( [<F $member>] ),*, $( [<R $member>] ),*>(
                    self,
                    $( [<f $member:snake:lower>]: [<F $member>] ),*
                ) -> $name<$( [<R $member>] ),*>
                where
                    $( [<F $member>]: Fn([<T $member>]) -> [<R $member>] ),*
                {
                    self.fold_into(
                        $( |x| $name::[<$member:camel>]([<f $member:snake:lower>](x)) ),*
                    )
                }
            }

        }
    }
}

// Define a coproduct, e.g.
// define_coprod_enum! { Foo {A, B} }
#[macro_export]
macro_rules! define_coprod {
    ($name:ident { $($member:ident),+ }) => {
        define_coprod_enum!{$name { $($member),+ }}
        define_coprod_impl!{$name { $($member),+ }}
    }
}
