import classNames from 'classnames';
import * as React from 'react';

const Container = props => {
    const containerClasses = classNames(props.className, {
        darkBackground: props.background === 'dark',
        highlightBackground: props.background === 'highlight',
        lightBackground: props.background === 'light',
        paddingAll: props.padding.indexOf('all') >= 0,
        paddingBottom: props.padding.indexOf('bottom') >= 0,
        paddingLeft: props.padding.indexOf('left') >= 0,
        paddingRight: props.padding.indexOf('right') >= 0,
        paddingTop: props.padding.indexOf('top') >= 0,
    });
    let wrappedChildren;

    if (props.wrapper) {
        wrappedChildren = <div className="container">{props.children}</div>;
    } else {
        wrappedChildren = props.children;
    }
    return (
        <div className={containerClasses} id={props.id}>
            {wrappedChildren}
        </div>
    );
};

Container.defaultProps = {
    background: null,
    padding: [],
    wrapper: true,
};

export default Container;
