use crate::utils::OrNone;
use humansize::{format_size, BINARY};
use patternfly_yew::prelude::*;
use spdx_rs::models::{PackageInformation, Relationship, SPDX};
use std::collections::HashMap;
use std::rc::Rc;
use yew::prelude::*;

pub fn spdx_creator(bom: &SPDX) -> Html {
    let title = html!(<Title>{"Creation"}</Title>);

    html!(
        <Card {title}>
            <CardBody>
                <DescriptionList>
                    <DescriptionGroup term="Created">{ &bom.document_creation_information.creation_info.created.to_string() }</DescriptionGroup>
                    if let Some(info) = &bom.document_creation_information.creation_info.license_list_version {
                        <DescriptionGroup term="License List Version">{ &info.to_string() }</DescriptionGroup>
                    }
                    {
                        match bom.document_creation_information.creation_info.creators.len() {
                            0 => html!(),
                            1 => {
                                // we can be sure to have one
                                html!(
                                    <DescriptionGroup term="Creator">
                                        { bom.document_creation_information.creation_info.creators[0].clone() }
                                    </DescriptionGroup>
                                )
                            },
                            _ => html! (
                                <DescriptionGroup term="Creators">
                                    <List>
                                        { for bom.document_creation_information.creation_info.creators.iter().map(Html::from) }
                                    </List>
                                </DescriptionGroup>
                            )
                        }
                    }
                </DescriptionList>
            </CardBody>
            { bom.document_creation_information.creation_info.creator_comment.as_ref().map(|comment|{
                html_nested!(<CardBody> { comment } </CardBody>)
            })}
        </Card>
    )
}

pub fn spdx_meta(bom: &SPDX) -> Html {
    let title = html!(<Title>{"Metadata"}</Title>);

    html!(
        <Card {title}>
            <CardBody>
                <DescriptionList>
                    <DescriptionGroup term="Name">{ &bom.document_creation_information.document_name }</DescriptionGroup>
                    <DescriptionGroup term="ID">{ &bom.document_creation_information.spdx_identifier }</DescriptionGroup>
                    <DescriptionGroup term="Namespace">{ &bom.document_creation_information.spdx_document_namespace }</DescriptionGroup>
                    <DescriptionGroup term="SPDX Version">{ &bom.document_creation_information.spdx_version }</DescriptionGroup>
                    <DescriptionGroup term="Data License">{ &bom.document_creation_information.data_license }</DescriptionGroup>
                </DescriptionList>
            </CardBody>
            { bom.document_creation_information.document_comment.as_ref().map(|comment|{
                html_nested!(<CardBody> { comment } </CardBody>)
            })}
        </Card>
    )
}

pub fn spdx_main(bom: &SPDX) -> Html {
    bom.document_creation_information
        .document_describes
        .iter()
        .map(|desc| {
            let title = html!(<Title>{ "Package" }</Title>);

            let content = match bom
                .package_information
                .iter()
                .find(|p| &p.package_spdx_identifier == desc)
            {
                Some(package) => {
                    vec![html!(
                    <DescriptionList>
                        <DescriptionGroup term="Name">{ &package.package_name }</DescriptionGroup>
                        <DescriptionGroup term="Version">{ OrNone(package.package_version.as_ref()) }</DescriptionGroup>
                        <DescriptionGroup term="External References"> { spdx_external_references(&package)} </DescriptionGroup>
                    </DescriptionList>
                )]
                },
                None => vec![
                    html!(
                            <CardBody>
                                <DescriptionList>
                                    <DescriptionGroup term="ID">{ &desc }</DescriptionGroup>
                                </DescriptionList>
                            </CardBody>

                    ),
                    html!(
                        <CardBody>
                            { "ID could not be found in document" }
                        </CardBody>
                    ),
                ],
            };

            html!(
                <Card {title}>
                    {
                        for content.into_iter()
                            .map(|content|html_nested!(<CardBody>{content}</CardBody>))
                    }
                </Card>
            )
        })
        .collect()
}

pub fn spdx_stats(size: usize, bom: &SPDX) -> Html {
    let title = html!(<Title>{"Statistics"}</Title>);
    html!(
        <Card {title}>
            <CardBody>
                <DescriptionList>
                    <DescriptionGroup term="Size">{ format_size(size, BINARY) }</DescriptionGroup>
                    <DescriptionGroup term="Packages">{ format!("{}", bom.package_information.len()) }</DescriptionGroup>
                </DescriptionList>
            </CardBody>
        </Card>
    )
}

#[derive(PartialEq, Properties)]
pub struct SpdxPackagesProperties {
    pub bom: Rc<SPDX>,
}

#[function_component(SpdxPackages)]
pub fn spdx_packages(props: &SpdxPackagesProperties) -> Html {
    #[derive(Clone, Eq, PartialEq)]
    enum Column {
        Name,
        Version,
    }

    #[derive(Clone, Debug, PartialEq)]
    struct PackageWrapper {
        package: PackageInformation,
        relations: Rc<Vec<Relationship>>,
        packages: Rc<HashMap<String, PackageInformation>>,
    }

    fn relationship_entry(packages: &HashMap<String, PackageInformation>, rel: &Relationship, id: &str) -> Html {
        html!(<>
            { OrNone(packages.get(id).map(|p| {

                match &p.package_version {
                    Some(version) => html!( <>
                        <Tooltip text={version.clone()}>
                            { &p.package_name }
                        </Tooltip>
                    </>),
                    None => {
                        html!(&p.package_name)
                    }
                }
                
            })) }
            {" "}
            <Label compact=true label={rel.relationship_type.as_ref().to_string()} />
        </>)
    }

    impl TableEntryRenderer<Column> for PackageWrapper {
        fn render_cell(&self, context: CellContext<'_, Column>) -> Cell {
            match context.column {
                Column::Name => html!(&self.package.package_name),
                Column::Version => html!(self.package.package_version.clone().unwrap_or_default()),
            }
            .into()
        }

        fn render_details(&self) -> Vec<Span> {
            let outgoing = self
                .relations
                .iter()
                .filter(|rel| rel.related_spdx_element == self.package.package_spdx_identifier)
                .collect::<Vec<_>>();
            let incoming = self
                .relations
                .iter()
                .filter(|rel| rel.spdx_element_id == self.package.package_spdx_identifier)
                .collect::<Vec<_>>();

            let content = html!(
                <Grid gutter=true>
                    <GridItem cols={[4]}>
                        <Card plain=true title={html!(<Title>{"Information"}</Title>)}>
                            <CardBody>
                                <DescriptionList>
                                    <DescriptionGroup term="Download">{ &self.package.package_download_location }</DescriptionGroup>
                                    <DescriptionGroup term="Copyright">{ &self.package.copyright_text }</DescriptionGroup>
                                    <DescriptionGroup term="License (declared)">{ &self.package.declared_license }</DescriptionGroup>
                                    <DescriptionGroup term="License (concluded)">{ &self.package.concluded_license }</DescriptionGroup>
                                </DescriptionList>
                            </CardBody>
                        </Card>
                    </GridItem>

                    <GridItem cols={[4]}>
                        <Card plain=true title={html!(<Title>{"External References"}</Title>)}>
                            <CardBody>
                                { spdx_external_references(&self.package) }
                            </CardBody>
                        </Card>
                    </GridItem>

                    <GridItem cols={[4]}>
                        <Card plain=true title={html!(<Title>{"Relationships"}</Title>)}>
                            {if !outgoing.is_empty() {
                                Some(html_nested!(<CardBody>
                                    <Title level={Level::H3}>{"Outgoing"}</Title>
                                    <List r#type={ListType::Basic}>
                                        { for outgoing.into_iter().map(|rel|relationship_entry(&self.packages, rel, &rel.spdx_element_id))}
                                    </List>
                                </CardBody>))
                            } else { None } }
                            { if !incoming.is_empty() {
                                Some(html_nested!(<CardBody>
                                    <Title level={Level::H3}>{"Incoming"}</Title>
                                    <List r#type={ListType::Basic}>
                                        { for incoming.into_iter().map(|rel|relationship_entry(&self.packages, rel, &rel.related_spdx_element))}
                                    </List>
                                </CardBody>))
                            } else { None } }
                        </Card>
                    </GridItem>
                </Grid>
            );

            vec![Span::max(content)]
        }
    }

    let header = html_nested!(
        <TableHeader<Column>>
            <TableColumn<Column> index={Column::Name} label="Name" />
            <TableColumn<Column> index={Column::Version} label="Version" />
        </TableHeader<Column>>
    );

    let package_map = use_memo(
        |bom| {
            bom.package_information
                .iter()
                .map(|p| (p.package_spdx_identifier.clone(), p.clone()))
                .collect::<HashMap<_, _>>()
        },
        props.bom.clone(),
    );

    let offset = use_state_eq(|| 0);
    let limit = use_state_eq(|| 10);

    let filter = use_state_eq(String::new);

    let filtered_packages = {
        let offset = offset.clone();
        let limit = limit.clone();
        use_memo(
            move |(bom, filter)| {
                let mut packages = bom
                    .package_information
                    .clone()
                    .into_iter()
                    // apply filter
                    .filter(|p| filter.is_empty() || p.package_name.contains(filter))
                    .collect::<Vec<_>>();

                // we need to sort after filtering, as paging requires a sorted list
                packages.sort_unstable_by(|a, b| a.package_name.cmp(&b.package_name));

                // try to cap last page, only apply once
                if *offset > packages.len() {
                    if *limit > packages.len() {
                        offset.set(0);
                    } else {
                        offset.set(packages.len() - *limit);
                    }
                }

                // return result
                packages
            },
            (props.bom.clone(), (*filter).clone()),
        )
    };

    // total entries must be based on the filtered list
    let total_entries = filtered_packages.len();

    let entries = use_memo(
        |(bom, filtered_packages, package_map, offset, limit)| {
            let relations = Rc::new(bom.relationships.clone());
            filtered_packages
                .iter()
                // apply pagination window
                .skip(*offset)
                .take(*limit)
                // map
                .map(|package| PackageWrapper {
                    package: (*package).clone(),
                    relations: relations.clone(),
                    packages: package_map.clone(),
                })
                .collect::<Vec<_>>()
        },
        (
            props.bom.clone(),
            filtered_packages.clone(),
            package_map,
            *offset,
            *limit,
        ),
    );

    let (entries, onexpand) = use_table_data(MemoizedTableModel::new(entries));

    // FIXME: if the following is missing, expansion is broken, figure out "why"
    let onexpand = onexpand.reform(|(key, state)| (key, state));

    let limit_callback = {
        let limit = limit.clone();
        Callback::from(move |number| limit.set(number))
    };

    let nav_callback = {
        let offset = offset.clone();
        let limit = *limit;
        Callback::from(move |page: Navigation| {
            let o = match page {
                Navigation::First => 0,
                Navigation::Last => ((total_entries - 1) / limit) * limit,
                Navigation::Previous => *offset - limit,
                Navigation::Next => *offset + limit,
                Navigation::Page(n) => (n - 1) * limit,
            };
            offset.set(o);
        })
    };

    let onclearfilter = {
        let filter = filter.clone();
        Callback::from(move |_| filter.set(String::new()))
    };

    let onsetfilter = {
        let filter = filter.clone();
        Callback::from(move |value: String| filter.set(value.trim().to_string()))
    };

    html!(
        <>
            <Toolbar>
                <ToolbarContent>
                    <ToolbarItem r#type={ToolbarItemType::SearchFilter}>
                        <TextInputGroup>
                            <TextInputGroupMain
                                placeholder="Filter"
                                icon={Icon::Search}
                                value={(*filter).clone()}
                                oninput={onsetfilter}
                            />
                            if !filter.is_empty() {
                                <TextInputGroupUtilities>
                                    <Button icon={Icon::Times} variant={ButtonVariant::Plain} onclick={onclearfilter}/>
                                </TextInputGroupUtilities>
                            }
                        </TextInputGroup>
                    </ToolbarItem>

                    <ToolbarItem r#type={ToolbarItemType::Pagination}>
                        <Pagination
                            {total_entries}
                            offset={*offset}
                            entries_per_page_choices={vec![5, 10, 25, 50]}
                            selected_choice={*limit}
                            onlimit={&limit_callback}
                            onnavigation={&nav_callback}
                        />
                    </ToolbarItem>
                </ToolbarContent>
            </Toolbar>

            <Table<Column, UseTableData<Column, MemoizedTableModel<PackageWrapper>>>
                mode={TableMode::CompactExpandable}
                {header}
                {entries}
                {onexpand}
            />

            <Pagination
                {total_entries}
                offset={*offset}
                entries_per_page_choices={vec![5, 10, 25, 50]}
                selected_choice={*limit}
                onlimit={&limit_callback}
                onnavigation={&nav_callback}
                position={PaginationPosition::Bottom}
            />
        </>
    )
}

/// render the external packages
fn spdx_external_references(package: &PackageInformation) -> Html {
    html!(
        <List>
            { for package
                .external_reference
                .iter()
                .map(|e| {
                    html!( <>
                    {&e.reference_locator} { " " }
                    <Label label={format!("{:?}", e.reference_category)} color={Color::Blue} /> { " " }
                    <Label label={format!("{}", e.reference_type)} color={Color::Grey} />
                </> )
                })
            }
        </List>
    )
}
