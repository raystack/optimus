import classNames from 'classnames';
import * as React from 'react';

class GridBlock extends React.Component {
    renderBlock(origBlock) {
        const blockDefaults = {
            imageAlign: 'left',
        };

        const block = {
            ...blockDefaults,
            ...origBlock,
        };

        const blockClasses = classNames('blockElement', this.props.className, {
            alignCenter: this.props.align === 'center',
            alignRight: this.props.align === 'right',
            fourByGridBlock: this.props.layout === 'fourColumn',
            threeByGridBlock: this.props.layout === 'threeColumn',
            twoByGridBlock: this.props.layout === 'twoColumn',
        });

        return (
            <div className={blockClasses} key={block.title}>
                <div className="blockContent">
                    {this.renderBlockTitle(block.title)}
                    {block.content}
                </div>
            </div>
        );
    }

    renderBlockTitle(title) {
        if (!title) {
            return null;
        }

        return <h2>{title}</h2>;
    }

    render() {
        return (
            <div className="gridBlock">
                {this.props.contents.map(this.renderBlock, this)}
            </div>
        );
    }
}

GridBlock.defaultProps = {
    align: 'left',
    contents: [],
    layout: 'twoColumn',
};

export default GridBlock;
